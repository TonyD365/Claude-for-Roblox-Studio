-- 运行 / 测试 handler（RunService 物理仿真，无本地玩家）。
-- start_test 还会往 ServerScriptService 注入一个 server-agent 脚本，让游戏跑起来后
-- 能在 server 上下文里执行 run_luau / fire_signal（见 agent/ServerAgentSource）。

local RunService = game:GetService("RunService")
local ServerScriptService = game:GetService("ServerScriptService")
local HttpService = game:GetService("HttpService")
local AgentSource = require(script.Parent.Parent.agent.ServerAgentSource)

local AGENT_NAME = "ClaudeServerAgent"

local M = {}

local function state(): string
	if RunService:IsRunning() then
		return "Running"
	elseif not RunService:IsEdit() then
		return "Paused"
	end
	return "Edit"
end

local function removeAgent()
	local existing = ServerScriptService:FindFirstChild(AGENT_NAME)
	if existing then
		existing:Destroy()
	end
end

-- 注入 server-agent（编辑态进行；游戏一跑它就作为服务器脚本执行）。
local function injectAgent(agentInfo: any): boolean
	removeAgent()
	if not agentInfo or not agentInfo.token or not agentInfo.port then
		return false
	end
	local ok = pcall(function()
		local agent = Instance.new("Script")
		agent.Name = AGENT_NAME
		agent:SetAttribute("ClaudeToken", tostring(agentInfo.token))
		agent:SetAttribute("ClaudePort", tonumber(agentInfo.port) or agentInfo.port)
		agent:SetAttribute("ClaudeSession", "agent-" .. HttpService:GenerateGUID(false))
		agent.Source = AgentSource.source
		pcall(function()
			agent.RunContext = Enum.RunContext.Server
		end)
		agent.Parent = ServerScriptService
	end)
	return ok
end

function M.start_test(args: any)
	local agentInjected = injectAgent(args and args.agent)
	-- RunService:Run() 会阻塞，所以需要在独立线程里执行，避免阻塞 WebSocket 消息处理。
	task.spawn(function()
		RunService:Run()
	end)
	task.wait() -- 让运行状态切换落定一帧，再读回真实状态。
	local s = state()
	local res: { [string]: any } = {
		requested = "Running",
		state = s,
		isRunning = RunService:IsRunning(),
		agentInjected = agentInjected,
	}
	if s ~= "Running" then
		res.ok = false
		res.warning = "RunService:Run() did not enter the Running state. "
			.. "Some places require pressing Run/Play in the Studio toolbar once, or HttpService/plugin "
			.. "permissions may be blocking it. Verify with get_run_state before relying on it."
	end
	return res
end

function M.stop_test()
	RunService:Stop()
	removeAgent()
	return { state = "Edit" }
end

function M.pause_test()
	RunService:Pause()
	return { state = "Paused" }
end

function M.get_run_state()
	return { state = state(), isEdit = RunService:IsEdit(), isRunning = RunService:IsRunning() }
end

return M
