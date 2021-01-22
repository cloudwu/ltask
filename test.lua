local man = require "ltask.schedule"
local ltask = require "ltask"

local MESSAGE_INIT <const> = 0
local MESSAGE_REQUEST <const> = 1
local MESSAGE_RESPONSE <const> = 2
local MESSAGE_POST <const> = 3
local MESSAGE_ERROR <const> = 4
local MESSAGE_BLOCK <const> = 5

print "Start ltask"

local config = man.init {
	service = "lualib/service.lua",
	root = "lualib/root.lua",
}

for k,v in pairs(config) do
	print(k,v)
end

local root = man.newservice(config.service)
assert(root == 1)

local msg, sz = ltask.pack(config.root)

-- init message
man.message {
	from = 0,
	to = root,
	session = 0,
	type = MESSAGE_INIT,
	message = msg,
	size = sz,
}
