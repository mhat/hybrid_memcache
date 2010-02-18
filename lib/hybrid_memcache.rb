require 'memcached'

class Memcache < Memcached
  WRITE_LOCK_WAIT = 1
  LOCK_TIMEOUT    = 5
  
  def initialize (opts={})
    servers    = opts[:servers] && opts.delete(:servers)
    @namespace = ""
    
    super(servers, {
      :prefix_key           =>  '',
      :prefix_delimiter     =>  '',
      :support_cas          => true,
      
      :hash                 => :fnv1_32,
      :distribution         => :consistent_ketama,
      :ketama_weighted      => true,
      :server_failure_limit => 2,
      :retry_timeout        => 30,
      
      :default_ttl          => 604800,
    }.merge!(opts))
  end


  def clone
     klone = self.class.new({ :servers => servers }.merge(options))
     klone.set_namespace @namespace
     klone
  end


  def add(key, value, opts={})
    super(normalize_keys(key), value, *opts_to_params(opts))
    return value
  rescue Memcached::NotStored
    return nil
  end

  
  def inspect
    "<Memcache: %d servers, ns: %p" % [ servers.length, namespace ]
  end


  def in_namespace(ns)
    # Temporarily change the namespace for convenience.
    begin
      old_namespace = @namespace
      self.set_namespace("#{old_namespace}#{ns}")
      yield
    ensure
      self.set_namespace(old_namespace)
    end
  end


  def namespace 
    @namespace
  end

  
  def set_namespace(ns)
    @namespace = ns
  end
  alias     namespace= set_namespace
  alias set_namespace= set_namespace


  def get(keys, opts={})
    marshal = !opts[:raw]
    cas     =  opts[:cas]
    
    unless keys.is_a?(Array)
      ## ninjudd-memcache has a weird behaviour where get can be called with an
      ## expiry and that will transform the get into get+cas. this in turn has
      ## the effect of extending the expiry of the object. 
      if opts[:expiry]
        value = get(keys,        :cas => true)
        value = cas(keys, value, :cas => value.memcache_cas, :expiry => opts[:expiry])
        return value
      end
      
      ## Single get
      value, flags, ret = Lib.memcached_get_rvalue(@struct, normalize_keys(keys))
      
      ## ninjudd-memcache treats broken servers as cache missis, so return nil
      check_return_code(ret, keys)
      return nil unless ret == 0
      
      if marshal
        value = Marshal.load(value) 
      end

      value.memcache_cas   = cas ? @struct.result.cas : false
      value.memcache_flags = flags
      return value
    else
      ## Multi get
      return {} if keys.empty?

      ## ninjudd-memcache normalizes keys into the form namespace:index:key
      ## but it hides this form from the caller, so the caller expects to 
      ## get a hash with the keys in their denormalized form. That's what
      ## the norm_to_std hash is all about. 
      normalized  = normalize_keys(keys)
      norm_to_std = {}
      
      ## but note, the keys have to be transformed into strings, even if they
      ## started out as fixnums
      keys.each_with_index {|k,idx| norm_to_std[normalized[idx]] = keys[idx].to_s }
      
      ret = Lib.memcached_mget(@struct, normalized)
      
      ## once again: potentiall braken server == cache miss
      check_return_code(ret, normalized)
      return {} unless ret == 0
    
      hash = {}
      keys.each do
        value, key, flags, ret = Lib.memcached_fetch_rvalue(@struct)
        if ret == Lib::MEMCACHED_END
          break 
        end
        check_return_code(ret, key)
        
        # Assign the value
        if marshal
          value = Marshal.load(value) 
        end 
        value.memcache_cas   = cas ? @struct.result.cas : false
        value.memcache_flags = flags
        
        hash[ norm_to_std[key] ] = value
      end
      return hash
    end
  rescue Memcached::NotFound
    return nil
  end


  def read(keys, opts={})
    get(keys, opts.merge(:raw => true))
  end


  def set(key, value, opts={})
    super(normalize_keys(key), value, *opts_to_params(opts))
    return value
  rescue Memcache::NotFound
    return nil
  end


  def write(key, value, opts={})
    set(key, value, opts.merge(:raw => true))
  end


  def replace(key, value, opts={})
    super(normalize_keys(key), value, *opts_to_params(opts))
    return value
  rescue Memcache::NotStored
    return nil
  end


  def cas(key, value, opts={})
    ttl, marshal, flags = opts_to_params(opts)
    key  = normalize_keys(key)
    data = marshal ? Marshal.dump(value) : value

    check_return_code(Lib.memcached_cas(@struct, key, data, ttl, flags, opts[:cas]), key)
    value.memcache_cas   = @struct.result.cas
    value.memcache_flags = @struct.result.flags
    return value
  rescue Memcache::NotStored
   return nil
  end


  def prepend(key,value)
    super(normalize_keys(key), value)
    return true
  rescue Memcache::NotStored
    return false
  end


  def append(key, value)
    super(normalize_keys(key), value)
    return true
  rescue Memcache::NotStored
    return false
  end


  def count(key)
    get(key, :raw => true).to_i
  end


  def increment(key, amount=1)
    super(normalize_keys(key), amount)
  rescue Memcache::NotStored
    return nil
  end
  alias incr increment


  def decrement(key, amount=1)
    super(normalize_keys(key), amount)
  rescue Memcache::NotStored
    return nil
  end
  alias decr decrement


  def update(key, opts={})
    if value = get(key, :cas => true)
      cas(key, yield(value), opts.merge!(:cas => value.memcache_cas))
    else
      add(key, yield(value), opts)
    end
  end


  def get_or_add(key, *args)
    if block_given?
      opts = args[0] || {}
      get(key) || add(key, yield,   opts) || get(key)
    else
      opts = args[1] || {}
      get(key) || add(key, args[0], opts) || get(key)
    end
  end


  def get_or_set(key, *args)
    if block_given?
      opts = args[0] || {}
      get(key) || set(key, yield,   opts) || get(key)
    else
      opts = args[1] || {}
      get(key) || set(key, args[0], opts) || get(key)
    end
  end


  def get_some(keys, opts = {})
    keys    = keys.collect { |k| k.to_s }
    records = opts[:disable] ? {} : self.get(keys, opts)
    
    if opts[:validation]
      records.delete_if do |key, value|
        not opts[:validation].call(key, value)
      end
    end
    
    keys_to_fetch = keys - records.keys
    method        = opts[:overwrite] ? :set : :add
    if keys_to_fetch.any?
      yield(keys_to_fetch).each do |key, value|
        self.send(method, key, value, opts) unless opts[:disable] or opts[:disable_write]
        records[key] = value
      end
    end
    records
  end


  def lock_key(key)
    "lock:#{key}"
  end


  def lock(key, opts={})
    expiry = opts[:expiry] || LOCK_TIMEOUT
    add(lock_key(key), Socket.gethostname, :expiry => expiry, :raw => true)
  end


  def unlock(key)
    delete(lock_key(key))
  end


  def with_lock(key, opts={})
    until lock(key) do
      return if opts[:ignore]
      sleep(WRITE_LOCK_WAIT)
    end
    yield
    unlock(key) unless opts[:keep]
  end


  def locked?(key)
    get(lock_key(key), :raw => true)
  end


  def delete(key)
    super(normalize_keys(key))
    return true
  rescue Memcached::NotFound
    return false
  end


  def flush_all(opts={})
    flush
  end
  alias clear flush_all


  def [](key)
    get(key)
  end


  def []=(key,value)
    set(key,value)
  end


  def normalize_keys (keys)
    ns = @namespace.nil? || @namespace.size == 0 ? "" : "#{@namespace}:"
    
    unless keys.is_a?(Array) 
      k = "#{ns}#{keys}"
      k.gsub!(/%/, '%%') if k.include?('%')
      k.gsub!(/ /, '%s') if k.include?(' ')
      return k
    else
      return keys.collect do |k| 
        k = "#{ns}#{k}" 
        k.gsub!(/%/, '%%') if k.include?('%')
        k.gsub!(/ /, '%s') if k.include?(' ')
        k
      end
    end
  end


  def opts_to_params (opts={})
    ## - if no :expiry, use @default_ttl
    ## - fauna-memcached uses marshal which has the opposite meaning of 
    ##   ninjudd's :raw. that is, :raw means DO NOT marshal, so invert
    ##   :raw via ! ... 
    ## - if no :flags,  use FLAGS
    return  opts[:expiry] || @default_ttl,
           !opts[:raw],
            opts[:flags]  || FLAGS
  end
end

class Object
  attr_accessor :memcache_flags, :memcache_cas
end
