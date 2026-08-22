-- Roickbot 插件入口：工具栏、状态面板、配对、WebSocket 连接。
-- 传输用 HttpService:CreateWebStreamClient(WebSocket)（Studio 持久双向连接），取代旧的长轮询。

local HttpService = game:GetService("HttpService")

local Config = require(script.Config)
local Dispatcher = require(script.Dispatcher)

local SETTING_TOKEN = "roickbot_token"
local SETTING_PORT = "roickbot_port"

local running = false
local sessionId = HttpService:GenerateGUID(false)

local function getToken(): string
	return plugin:GetSetting(SETTING_TOKEN) or ""
end
local function getPort(): number
	return tonumber(plugin:GetSetting(SETTING_PORT)) or Config.DefaultPort
end
local function wsUrl(): string
	return "ws://127.0.0.1:" .. tostring(getPort()) .. "/ws"
end

------------------------------------------------------------------- UI
-- 品牌配色（与桌面 App 一致）。
local C = {
	bg = Color3.fromRGB(23, 22, 31),
	surface = Color3.fromRGB(14, 13, 20),
	brand = Color3.fromRGB(124, 108, 246),
	brandHover = Color3.fromRGB(140, 125, 255),
	coral = Color3.fromRGB(242, 160, 136),
	text = Color3.fromRGB(236, 235, 245),
	muted = Color3.fromRGB(155, 152, 179),
	faint = Color3.fromRGB(111, 108, 133),
	good = Color3.fromRGB(70, 201, 139),
	bad = Color3.fromRGB(240, 97, 109),
	idle = Color3.fromRGB(245, 181, 68),
	line = Color3.fromRGB(48, 46, 62),
}

local function corner(inst: Instance, r: number)
	local c = Instance.new("UICorner")
	c.CornerRadius = UDim.new(0, r)
	c.Parent = inst
end
local function padAll(inst: Instance, l: number, t: number, r: number, b: number)
	local p = Instance.new("UIPadding")
	p.PaddingLeft = UDim.new(0, l)
	p.PaddingTop = UDim.new(0, t)
	p.PaddingRight = UDim.new(0, r)
	p.PaddingBottom = UDim.new(0, b)
	p.Parent = inst
end
local function stroke(inst: Instance, color: Color3, thickness: number, transparency: number)
	local s = Instance.new("UIStroke")
	s.Color = color
	s.Thickness = thickness
	s.Transparency = transparency
	s.Parent = inst
end

local toolbar = plugin:CreateToolbar("Roickbot")
local toggleButton = toolbar:CreateButton(
	"Roickbot",
	"Open the Roickbot panel",
	"rbxasset://textures/ui/GuiImagePlaceholder.png"
)
toggleButton.ClickableWhenViewportHidden = true

local widgetInfo = DockWidgetPluginGuiInfo.new(Enum.InitialDockState.Float, false, true, 320, 360, 300, 320)
local widget = plugin:CreateDockWidgetPluginGui("RoickbotPanel", widgetInfo)
widget.Title = "Roickbot"

local root = Instance.new("Frame")
root.Size = UDim2.fromScale(1, 1)
root.BackgroundColor3 = C.bg
root.BorderSizePixel = 0
root.Parent = widget

local layout = Instance.new("UIListLayout")
layout.Padding = UDim.new(0, 12)
layout.SortOrder = Enum.SortOrder.LayoutOrder
layout.FillDirection = Enum.FillDirection.Vertical
layout.Parent = root
padAll(root, 16, 16, 16, 16)

-- ---- header：logo + 标题 + 状态点 ----
local header = Instance.new("Frame")
header.Size = UDim2.new(1, 0, 0, 38)
header.BackgroundTransparency = 1
header.LayoutOrder = 1
header.Parent = root

local logo = Instance.new("Frame")
logo.Size = UDim2.fromOffset(34, 34)
logo.AnchorPoint = Vector2.new(0, 0.5)
logo.Position = UDim2.fromScale(0, 0.5)
logo.BackgroundColor3 = C.brand
logo.Parent = header
corner(logo, 9)
do
	local g = Instance.new("UIGradient")
	g.Rotation = 90
	g.Color = ColorSequence.new(Color3.fromRGB(138, 121, 255), Color3.fromRGB(91, 73, 230))
	g.Parent = logo
	local face = Instance.new("Frame") -- 旋转的方块，暗示立方体的一个面
	face.Size = UDim2.fromOffset(14, 14)
	face.AnchorPoint = Vector2.new(0.5, 0.5)
	face.Position = UDim2.fromScale(0.5, 0.5)
	face.Rotation = 45
	face.BackgroundColor3 = Color3.fromRGB(255, 255, 255)
	face.BackgroundTransparency = 0.12
	face.Parent = logo
	corner(face, 3)
	local spark = Instance.new("Frame") -- 珊瑚色火花点
	spark.Size = UDim2.fromOffset(9, 9)
	spark.AnchorPoint = Vector2.new(0.5, 0.5)
	spark.Position = UDim2.fromScale(1, 0)
	spark.BackgroundColor3 = C.coral
	spark.Parent = logo
	corner(spark, 5)
end

local titleLabel = Instance.new("TextLabel")
titleLabel.BackgroundTransparency = 1
titleLabel.Position = UDim2.new(0, 46, 0, 2)
titleLabel.Size = UDim2.new(1, -66, 0, 18)
titleLabel.Font = Enum.Font.GothamBold
titleLabel.TextSize = 15
titleLabel.TextXAlignment = Enum.TextXAlignment.Left
titleLabel.TextColor3 = C.text
titleLabel.Text = "Roickbot"
titleLabel.Parent = header

local subtitleLabel = Instance.new("TextLabel")
subtitleLabel.BackgroundTransparency = 1
subtitleLabel.Position = UDim2.new(0, 46, 0, 20)
subtitleLabel.Size = UDim2.new(1, -66, 0, 14)
subtitleLabel.Font = Enum.Font.Gotham
subtitleLabel.TextSize = 11
subtitleLabel.TextXAlignment = Enum.TextXAlignment.Left
subtitleLabel.TextColor3 = C.muted
subtitleLabel.Text = "AI ↔ Roblox Studio"
subtitleLabel.Parent = header

local statusDot = Instance.new("Frame")
statusDot.Size = UDim2.fromOffset(10, 10)
statusDot.AnchorPoint = Vector2.new(1, 0.5)
statusDot.Position = UDim2.new(1, 0, 0.5, 0)
statusDot.BackgroundColor3 = C.faint
statusDot.Parent = header
corner(statusDot, 5)

-- ---- token 输入 ----
local tokenLabel = Instance.new("TextLabel")
tokenLabel.Size = UDim2.new(1, 0, 0, 14)
tokenLabel.BackgroundTransparency = 1
tokenLabel.Font = Enum.Font.GothamMedium
tokenLabel.TextSize = 11
tokenLabel.TextXAlignment = Enum.TextXAlignment.Left
tokenLabel.TextColor3 = C.muted
tokenLabel.Text = "CONNECTION TOKEN"
tokenLabel.LayoutOrder = 2
tokenLabel.Parent = root

local tokenBox = Instance.new("TextBox")
tokenBox.Size = UDim2.new(1, 0, 0, 34)
tokenBox.LayoutOrder = 3
tokenBox.PlaceholderText = "Paste the token from the desktop app…"
tokenBox.PlaceholderColor3 = C.faint
tokenBox.Text = getToken()
tokenBox.ClearTextOnFocus = false
tokenBox.Font = Enum.Font.Code
tokenBox.TextSize = 12
tokenBox.TextColor3 = C.text
tokenBox.BackgroundColor3 = C.surface
tokenBox.TextXAlignment = Enum.TextXAlignment.Left
tokenBox.TextTruncate = Enum.TextTruncate.AtEnd
tokenBox.Parent = root
corner(tokenBox, 8)
stroke(tokenBox, C.line, 1, 0)
padAll(tokenBox, 10, 0, 10, 0)

-- ---- connect 按钮 ----
local connectBtn = Instance.new("TextButton")
connectBtn.Size = UDim2.new(1, 0, 0, 38)
connectBtn.LayoutOrder = 4
connectBtn.AutoButtonColor = false
connectBtn.Font = Enum.Font.GothamBold
connectBtn.TextSize = 14
connectBtn.TextColor3 = Color3.fromRGB(255, 255, 255)
connectBtn.BackgroundColor3 = C.brand
connectBtn.Text = "Connect"
connectBtn.Parent = root
corner(connectBtn, 10)
connectBtn.MouseEnter:Connect(function()
	connectBtn.BackgroundColor3 = C.brandHover
end)
connectBtn.MouseLeave:Connect(function()
	connectBtn.BackgroundColor3 = C.brand
end)

-- ---- 状态文字 ----
local statusLabel = Instance.new("TextLabel")
statusLabel.Size = UDim2.new(1, 0, 0, 60)
statusLabel.LayoutOrder = 5
statusLabel.BackgroundTransparency = 1
statusLabel.Font = Enum.Font.Gotham
statusLabel.TextSize = 12
statusLabel.TextWrapped = true
statusLabel.TextYAlignment = Enum.TextYAlignment.Top
statusLabel.TextXAlignment = Enum.TextXAlignment.Left
statusLabel.TextColor3 = C.muted
statusLabel.Text = "Disconnected."
statusLabel.Parent = root

-- 根据状态文字推断状态点/文字颜色。
local function setStatus(text: string)
	statusLabel.Text = text
	local low = string.lower(text)
	local dotColor, textColor = C.faint, C.muted
	if string.find(low, "connected", 1, true) and not string.find(low, "not ", 1, true) and not string.find(low, "dis", 1, true) then
		dotColor, textColor = C.good, C.good
	elseif
		string.find(low, "fail", 1, true)
		or string.find(low, "error", 1, true)
		or string.find(low, "reject", 1, true)
		or string.find(low, "unavailable", 1, true)
	then
		dotColor, textColor = C.bad, C.bad
	elseif string.find(low, "connecting", 1, true) or string.find(low, "reconnect", 1, true) then
		dotColor, textColor = C.idle, C.idle
	end
	statusDot.BackgroundColor3 = dotColor
	statusLabel.TextColor3 = textColor
end

local function refreshButton()
	connectBtn.Text = if running then "Disconnect" else "Connect"
end

------------------------------------------------------------------- WebSocket 连接
local client: any = nil -- 当前 WebStreamClient
local generation = 0 -- 每次连接自增，用来识别"过期"连接的回调/重连

-- 通过 WS 发一条 JSON 消息（连接未就绪时静默忽略）。
local function send(tbl: any): boolean
	if not client then
		return false
	end
	local ok = pcall(function()
		client:Send(HttpService:JSONEncode(tbl))
	end)
	return ok
end

local function handleMessage(message: string)
	local ok, m = pcall(function()
		return HttpService:JSONDecode(message)
	end)
	if not ok or type(m) ~= "table" then
		return
	end
	if m.type == "command" then
		local env = m.payload
		setStatus("Running: " .. tostring(env.tool))
		local response = Dispatcher.dispatch(env)
		response.type = "response"
		send(response)
		setStatus("Connected (WebSocket). Last: " .. tostring(env.tool))
	elseif m.type == "handshake_ok" then
		setStatus("Connected (WebSocket). Waiting for commands…")
	end
end

local startConnection -- 前向声明（Closed 回调里重连用）

startConnection = function()
	if not running then
		return
	end
	local token = getToken()
	if token == "" then
		running = false
		refreshButton()
		setStatus("Paste the token from the desktop app first.")
		return
	end
	if not (HttpService :: any).CreateWebStreamClient then
		running = false
		refreshButton()
		setStatus("This Studio build has no WebSocket support. Enable it in File ▸ Beta Features, then reconnect.")
		return
	end

	generation += 1
	local gen = generation

	-- 重连前先关掉上一条连接：Studio 进入 Test 时经常直接丢弃 WebSocket 而不发关闭帧，
	-- 桥那边会以为它还开着；两条连接同时挂在同一个通道上会把桌面 App 卡死。
	if client then
		local stale = client
		client = nil
		pcall(function()
			stale:Close()
		end)
	end

	setStatus("Connecting (WebSocket)…")

	local ok, c = pcall(function()
		return HttpService:CreateWebStreamClient(Enum.WebStreamClientType.WebSocket, {
			Url = wsUrl(),
			Headers = { Authorization = "Bearer " .. token, ["X-Roblox-MCP"] = "1" },
		})
	end)
	if not ok or not c then
		-- 多半是实验性功能未开启；Roblox 没有从代码开启 Beta Feature 的 API，只能引导用户手开。
		setStatus("WebSocket unavailable (" .. tostring(c) .. "). Enable it in File ▸ Beta Features and reconnect.")
		if running then
			task.wait(3)
			if running and gen == generation then
				startConnection()
			end
		end
		return
	end

	client = c

	c.Opened:Connect(function()
		if gen ~= generation then
			return
		end
		send({
			type = "handshake",
			pluginVersion = Config.PluginVersion,
			sessionId = sessionId,
			placeId = game.PlaceId,
			tools = Dispatcher.toolNames(),
		})
		setStatus("Connected (WebSocket). Waiting for commands…")
	end)

	c.MessageReceived:Connect(function(message: string)
		if gen == generation then
			handleMessage(message)
		end
	end)

	c.Error:Connect(function(code: number, errorMessage: string)
		if gen == generation then
			setStatus("WebSocket error " .. tostring(code) .. ": " .. tostring(errorMessage))
		end
	end)

	c.Closed:Connect(function()
		if gen ~= generation then
			-- 已被新连接取代的旧 socket：不要把当前 client 置空。
			return
		end
		client = nil
		if running then
			setStatus("Disconnected — reconnecting…")
			task.wait(1)
			if running and gen == generation then
				startConnection()
			end
		else
			setStatus("Disconnected.")
		end
	end)
end

local function connect()
	if running then
		return
	end
	running = true
	refreshButton()
	startConnection()
end

local function disconnect()
	running = false
	generation += 1 -- 让所有旧回调/重连失效
	refreshButton()
	if client then
		pcall(function()
			client:Close()
		end)
		client = nil
	end
	setStatus("Disconnected.")
end

------------------------------------------------------------------- 事件
tokenBox.FocusLost:Connect(function()
	plugin:SetSetting(SETTING_TOKEN, tokenBox.Text)
end)

connectBtn.Activated:Connect(function()
	if running then
		disconnect()
	else
		connect()
	end
end)

toggleButton.Click:Connect(function()
	widget.Enabled = not widget.Enabled
end)

plugin.Unloading:Connect(function()
	running = false
end)

------------------------------------------------------------------- 运行状态监听
-- 监听 Edit/Running/Paused 变化，主动 push 事件（让 wait_for_event 能感知测试结束/状态切换）。
do
	local RunService = game:GetService("RunService")
	local function stateStr(): string
		if RunService:IsRunning() then
			return "Running"
		elseif not RunService:IsEdit() then
			return "Paused"
		end
		return "Edit"
	end
	task.spawn(function()
		local last = stateStr()
		while true do
			task.wait(0.5)
			if running then
				local s = stateStr()
				if s ~= last then
					last = s
					send({
						type = "event",
						event = { type = "runState", state = s, isRunning = RunService:IsRunning() },
					})
				end
			end
		end
	end)

	-- 把运行时报错也推成事件，wait_for_event 可一次等到"出错了"。
	-- 节流：最多每秒 1 条，避免错误风暴。
	local LogService = game:GetService("LogService")
	local lastErrAt = -1
	LogService.MessageOut:Connect(function(message: string, messageType: Enum.MessageType)
		if running and messageType == Enum.MessageType.MessageError then
			local now = os.clock()
			if now - lastErrAt < 1 then
				return
			end
			lastErrAt = now
			send({ type = "event", event = { type = "output", messageType = "Error", message = message } })
		end
	end)
end

-- 已配对（存有 token）则开 Studio 自动连接。
if getToken() ~= "" then
	connect()
end
