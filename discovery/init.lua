---@module 'discovery'

---@type DiscoveryPool
local M = {
	_VERSION = '0.10.1',
}
local log = require 'log'
local fiber = require 'fiber'
local netbox = require 'net.box'
local background = require 'background'

---@class DiscoveryTarantool
---@field addr DiscoveryTarantoolURI URI to tarantool
---@field conn NetBoxConnection connection to tarantool
---@field connected boolean is connection is treated as alive
---@field on_the_fly number current on_the_fly requests
---@field timeout number (in seconds) timeout of netbox.requests
---@field async boolean (default: true) is connection must be established in async way
---@field reconnect_timeout number (seconds) timeout of sleep before reconnect
---@field closed boolean flag describes that connection was closed
---@field on_connect fun(DiscoveryTarantool)
---@field on_disconnect fun(DiscoveryTarantool)
local Tarantool = {}
Tarantool.__index = Tarantool

---Is Tarantool connected
---@return boolean
function Tarantool:is_connected()
	if type(self.conn) == 'table' and self.conn.is_connected then
		return self.conn:is_connected() and self.connected
	end
	return false
end

function Tarantool.new(_, opts)
	assert(opts.addr, "Tarantool:new: opts.addr is required")
	assert(opts.timeout, "Tarantool:new opts.timeout is required")

	local self = setmetatable(opts, Tarantool)
	self:connect()
	return self
end
setmetatable(Tarantool, { __call = Tarantool.new })

function Tarantool:connect()
	local conn = netbox.connect(self.addr, {
		reconnect_after = self.reconnect_timeout,
		wait_connected = self.async or false,
		connect_timeout = self.timeout,
	})

	self.on_the_fly = 0
	self.conn = conn
	self.connected = false

	conn:on_connect(function(_)
		fiber.create(function()
			fiber.name('dsc/cnt:'..conn.host..':'..conn.port)
			if self.conn ~= conn then conn:close() return end

			self.connected = true
			log.info("Connected to %s:%s", conn.host, conn.port)

			if self.on_connect then
				local ok, err = pcall(self.on_connect, self)
				if not ok then
					log.error("on_connect hook failed: %s", err)
				end
			end
		end)
	end)
	conn:on_disconnect(function(_)
		fiber.create(function()
			fiber.name('dsc/dsc:'..conn.host..':'..conn.port)
			if self.conn ~= conn then conn:close() return end

			self.connected = false
			log.info("Disconnected from %s:%s", conn.host, conn.port)

			if self.on_disconnect then
				local ok, err = pcall(self.on_disconnect, self)
				if not ok then
					log.error("on_disconnect hook failed: %s", err)
				end
			end
		end)
	end)
end

function Tarantool:reconnect()
	self.connected = false
	if self.conn then
		self.conn:close()
		self.conn = nil
	end
	if self.on_disconnect then self:on_disconnect() end
	self:connect()
end

local function tt_tail_call(self, ...)
	self.on_the_fly = self.on_the_fly - 1
	if self.closed then
		self.conn.connected = false
		self.conn:close()
	end
	return ...
end

function Tarantool:call(method, args, opts)
	self.on_the_fly = self.on_the_fly + 1
	return tt_tail_call(self, pcall(self.conn.call, self.conn, method, args, opts))
end

---@class DiscoveryOptions
---@field refresh_timeout number (default: 0.1) refresh timeout of calling discovery func
---@field net_box_timeout number (seconds, default: 1s) timeout of discovery timeout call
---@field method string (Lua func which will be called by discovery module)

---@class DiscoveryUpstreamETCD
---@field refresh_timeout number (default: none) timeout of refreshing etcd list
---@field prefix string prefix in ETCD tree to fetch instances

---@class DiscoveryUpstream
---@field net_box_timeout number (seconds, default: 1) default upstream timeout call
---@field endpoints string[] list of tarantool uris to be connected to
---@field reconnect_timeout number (seconds, default: 0.3) timeout of reconnect if upstream fails
---@field etcd DiscoveryUpstreamETCD

---@class DiscoveryProxyOptions
---@field weight number balancing weight
---@field retriable boolean? is method is retriable

---@alias DiscoveryTarantoolURI string

---@class DiscoveryPool
---@field autoconnect boolean (default: true) defines if connection will be established automatically
---@field upstream DiscoveryUpstream upstream configuration
---@field discovery DiscoveryOptions discovery configuration
---@field nodes table<DiscoveryTarantoolURI, DiscoveryTarantool> kv-map of upstream connections to Tarantools
---@field methods table<string,table<DiscoveryTarantoolURI,DiscoveryProxyOptions>> kv-map of methods to proxy options
---@field methods_list table<string, {addr: DiscoveryTarantoolURI, max_weight: number}> helper kv-list for balancer
---@field conds table<string, fiber.cond> kv-map of fiber.conds() for each method. Call waits on this cond.

---Creates new DiscoveryPool to Replicaset
---@param _ any
---@param args any
---@return DiscoveryPool
function M.new(_, args)
	assert(type(args) == 'table', "discovery: args must be a table")
	assert(type(args.discovery) == 'table', "discovery: args.discovery must be a table")
	assert(type(args.upstream) == 'table', "discovery: args.upstream must be a table")

	if not args.upstream.endpoints and not args.upstream.etcd then
		error("discovery: one of args.upstream.endpoints or args.upstream.etcd must be specified")
	end
	if args.upstream.endpoints and args.upstream.etcd then
		error("discovery: you must specify args.upstream.endpoints xor args.upstream.etcd")
	end

	args.upstream.net_box_timeout = args.upstream.net_box_timeout or 1

	if args.upstream.reconnect_timeout == nil then
		args.upstream.reconnect_timeout = 0.3
	end

	args.discovery.refresh_timeout = args.discovery.refresh_timeout or 0.1
	args.discovery.net_box_timeout = args.discovery.net_box_timeout or 1

	local self = setmetatable(args, {__index = M})
	self.nodes = {}
	self.methods = {}
	self.methods_list = {}
	self.conds = {}

	if self.autoconnect ~= false then
		if self.upstream.etcd then
			local config_module = args.config_module or require'config'
			self.etcd_f = background {
				name = 'discovery/etcd',
				wait = false,
				restart = true,
				run_interval = self.upstream.etcd.refresh_timeout,
				setup = function(ctx) ctx.endpoints = {} end,
				func = function(ctx)
					local result, _ = config_module.etcd:list(self.upstream.etcd.prefix)
					local endpoints = {}
					for _, info in pairs(result) do
						if not info.disabled then
							endpoints[info.box.remote_addr or info.box.listen] = true
						end
					end
					local do_connect
					for endpoint in pairs(endpoints) do
						if ctx.endpoints[endpoint] == nil then
							do_connect = true
							break
						end
					end
					for endpoint in pairs(ctx.endpoints) do
						if not endpoints[endpoint] then
							do_connect = true
							break
						end
					end

					if do_connect then
						ctx.endpoints = endpoints
						local list = {}
						for endpoint in pairs(ctx.endpoints) do
							table.insert(list, endpoint)
						end
						self:connect(list)

						for addr, tnt in pairs(self.nodes) do
							if not ctx.endpoints[addr] then
								self:expose(addr, tnt)
							end
						end
					end
				end,
			}
		else
			self:connect(self.upstream.endpoints)
		end
	end

	return self
end

function M:connect(endpoints)
	for _, addr in pairs(endpoints) do
		self.nodes[addr] = Tarantool {
			addr = addr,
			reconnect_timeout = self.upstream.reconnect_timeout,
			timeout = self.upstream.net_box_timeout,
			on_connect = function(tnt)
				self:on_connect(addr, tnt)
			end,
			on_disconnect = function(tnt)
				self:on_disconnect(addr, tnt)
			end,
		}
	end
end

---on_connect hook
---@param addr DiscoveryTarantoolURI
	---@param tnt DiscoveryTarantool
function M:on_connect(addr, tnt)
	tnt.discovery_f = background {
		name = 'discovery/'..tnt.conn.host..':'..tnt.conn.port,
		setup = function(ctx) ctx.tnt = tnt end,
		run_interval = self.discovery.refresh_timeout,
		restart = 2*math.max(self.discovery.refresh_timeout, self.discovery.net_box_timeout),
		wait = false, -- wait noone
		func = function(ctx)
			local res = ctx.tnt.conn:call(self.discovery.method, {}, { timeout = self.discovery.net_box_timeout })
			self:on_discovery(addr, res)
		end,
		on_fail = function(ctx, err)
			log.error("discovery failed %s", err)
			ctx.tnt:reconnect()
		end,
	}
end

function M:on_disconnect(addr, tnt)
	log.info("Gracefull shutdown for background discovery for %s", addr)
	tnt.discovery_f:shutdown()
	self:on_undiscovery(addr)
end

function M:expose(addr, tnt)
	if self.nodes[addr] ~= tnt then
		log.warn("attempt to expose wrong instance %s (exp %s, got %s)", addr, self.nodes[addr], tnt)
		return
	end

	tnt.discovery_f:shutdown()
	self:on_undiscovery(addr)
	self.nodes[addr] = nil
	tnt.closed = true
	if tnt.on_the_fly == 0 then
		tnt:close()
	end
end

function M:on_discovery(addr, res)
	assert(type(res) == 'table', "discovered response must be a table")
	assert(type(res.methods) == 'table', "discoverred response must contain methods")

	assert(self.nodes[addr].connected, "Node must be connected")

	local do_rebuild = false
	for method, info in pairs(res.methods) do
		if not self.methods[method] then
			self.methods[method] = {}
		end
		if not self.methods[method][addr] then
			self.methods[method][addr] = {}
		end
		for _, key in ipairs{"weight", "retriable"} do
			if self.methods[method][addr][key] ~= info[key] then
				do_rebuild = true
			end
		end
		self.methods[method][addr] = info
	end

	if do_rebuild then
		self:rebuild()
	end
end

function M:on_undiscovery(addr)
	self:rebuild()
	log.info("Methods of %s undiscovered", addr)
end

function M:rebuild()
	for method in pairs(self.methods) do
		for addr in pairs(self.methods[method]) do
			if not self.nodes[addr] then
				log.warn("Node %s is not registered in discovery but exists in balancer graph", addr)
				self.methods[method][addr] = nil
			elseif not self.nodes[addr]:is_connected() then
				log.warn("Disable %s:%s because disconnected", method, addr)
				self.methods[method][addr] = nil
			end
		end
		local list = {}
		local total_weight = 0
		local addrs = {}

		for node_addr, balance in pairs(self.methods[method]) do
			if balance.weight ~= 0 then
				total_weight = total_weight + balance.weight
				table.insert(list, { addr = node_addr, max_weight = total_weight, retriable = balance.retriable })
				table.insert(addrs, ("%s:%s"):format(node_addr, total_weight))
			end
		end

		self.methods_list[method] = list

		if #list > 0 then
			log.verbose("Rebuilded %s: %s", method, table.concat(addrs, ","))
			if self.conds[method] then
				self.conds[method]:broadcast()
			end
		end
	end
end

local time_tolerance_mks = 100

local function is_deadline_exceeded(deadline, now)
	now = now or fiber.time()
	return (now-deadline)*1e6 > time_tolerance_mks
end

local function tail_call(self, ctx, pcall_ok, ...)
	local now = fiber.time()
	ctx.total_time = now-ctx.started_at
	ctx.execution_time = now-ctx.executed_at

	if pcall_ok then
		log.verbose("[Proxy=ok] to %s in %.5fs (total: %.5fs)",
			ctx.addr, now-ctx.executed_at, now-ctx.started_at)
		return ...
	end

	log.error("call %s to {%s} (attempt=%s,retriable=%s,duration=%.3fs,total=%.3fs,left=%.3fs) failed with: %s",
		ctx.method, ctx.addr, ctx.attempt, ctx.retriable,
		ctx.execution_time,
		ctx.total_time,
		ctx.deadline - now,
		...
	)

	local err = ...
	ctx.last_error = box.error.new{
		type = 'DiscoveryError',
		code = M.errors.execution_failed,
		reason = tostring(err),
	}

	if ctx.retriable and not is_deadline_exceeded(ctx.deadline, fiber.time()+0.001) then
		-- local json = require 'json'
		-- log.warn("retrying call(%s, %s, %s, %s)",
		-- 	ctx.method, json.encode(ctx.args), json.encode(ctx.opts), json.encode(ctx))
		fiber.sleep(0.001)
		return self:call(ctx.method, ctx.args, ctx.opts, ctx)
	end

	ctx.last_error:raise()
end

M.errors = {
	no_route_for_call = 0x100,
	execution_timed_out = 0x101,
	execution_failed = 0x102,
}

---Calls given method on connection pool
---@param method string
---@param args any[]?
---@param opts table?
---@param ctx table?
---@return ...
function M:call(method, args, opts, ctx)
	args = args or {}
	opts = opts or {}
	opts.timeout = tonumber(opts.timeout) or self.upstream.net_box_timeout

	if type(ctx) ~= 'table' then
		ctx = {}
	end

	ctx.started_at = ctx.started_at or fiber.time()
	ctx.attempt = ctx.attempt or 0
	ctx.method = ctx.method or method
	ctx.max_attempts = ctx.max_attempts or tonumber(opts.max_attempts)
	ctx.args = ctx.args or args

	local deadline = ctx.deadline
	if not deadline then
		deadline = tonumber(opts.deadline)
	end
	if not deadline then
		if ctx.max_attempts then
			deadline = fiber.time() + opts.timeout * ctx.max_attempts
		else
			deadline = fiber.time() + opts.timeout
		end
	end

	if is_deadline_exceeded(deadline) then
		ctx.total_time = fiber.time() - ctx.started_at
		ctx.execution_time = ctx.execution_time or -1

		if ctx.last_error then
			ctx.last_error:raise()
		end

		box.error{
			reason = ("Timeout for discovery of %s exceeded"):format(method),
			type = 'DiscoveryError',
			code = M.errors.execution_timed_out,
		}
	end

	ctx.deadline = deadline
	opts.max_attempts = nil

	local balance = self.methods_list[method]
	if not balance or #balance == 0 then
		log.verbose("No nodes available for %s. Waiting %.3fs", method, deadline - fiber.time())
		self.conds[method] = self.conds[method] or fiber.cond()
		self.conds[method]:wait(deadline - fiber.time())
		if is_deadline_exceeded(deadline) then
			ctx.total_time = fiber.time() - ctx.started_at
			ctx.execution_time = ctx.execution_time or -1
			box.error{
				reason = ("No route for call %s exceeded"):format(method),
				type = 'DiscoveryError',
				code = M.errors.no_route_for_call,
			}
		end
		return self:call(method, args, opts, ctx)
	end

	local node, node_addr do
		if ctx.attempt == 0 then -- this is first attempt
			local rnd = math.random(0, balance[#balance].max_weight)
			for i = 1, #balance do
				if rnd <= balance[i].max_weight then
					node = balance[i]
					node_addr = node.addr
					break
				end
			end
		else
			if ctx.max_attempts and ctx.attempt == ctx.max_attempts then
				error(ctx.last_error)
			end
			if not ctx.balance_order then
				-- peak all ids except balance_start (it is already failed)
				local ids = table.new(#balance - 1, 0)
				local no = 1
				for i = 1, #balance do
					local addr = balance[i].addr
					if ctx.addr ~= addr then
						ids[no] = addr
						no = no + 1
					end
				end
				-- Fisher-Yates shuffle
				for i = #ids, 2, -1 do
					local j = math.random(i)
					ids[i], ids[j] = ids[j], ids[i] -- swap
				end
				ctx.balance_order = ids
			end

			-- circuit breaker (noone left to retry):
			if ctx.attempt > #ctx.balance_order then
				error(ctx.last_error)
			end

			-- this loop almost always will be executed once
			-- but if `balance` is rebuilded after last call
			-- then we might face some inconsistenct in ctx.balance_order
			-- values and indices in `balance`
			for _ = ctx.attempt, #ctx.balance_order do
				node_addr = ctx.balance_order[ctx.attempt]
				node = self.methods[method][node_addr]
				if node then
					break
				end
			end

			-- if node is not found then we reraise last error
			if not node then
				error(ctx.last_error)
			end
		end
	end

	assert(node)

	local tnt = self.nodes[node_addr]
	if not tnt:is_connected() then
		log.warn("Node %s which was choosen for %s has been disconnected",
			node.addr, method)
		self:rebuild()
		return self:call(method, args, opts, ctx)
	end

	ctx.retriable = node.retriable == true
	ctx.addr = node_addr
	ctx.attempt = ctx.attempt + 1
	opts.timeout = math.min(deadline - fiber.time(), opts.timeout)
	ctx.executed_at = fiber.time()
	opts.deadline = nil
	ctx.opts = opts

	log.verbose("Calling %s on %s (attempt #%d)", method, node.addr, ctx.attempt)
	return tail_call(self, ctx, tnt:call(method, args, opts))
end

setmetatable(M, {__call = M.new})
return M
