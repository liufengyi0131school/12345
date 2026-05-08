"""
简易WebSocket服务器 - 用于多人游戏的中转站
"""
import asyncio
import json
import websockets
from datetime import datetime

class GameServer:
    def __init__(self, host="0.0.0.0", port=8765):
        self.host = host
        self.port = port
        self.clients = set()  # 存储所有连接的客户端
        self.player_data = {}  # 存储玩家数据 {player_id: {x, y, chat, inventory}}
        self.world_state = {}  # 存储世界方块状态 {(x,y): block_id}
        
    async def register_client(self, websocket):
        """注册新客户端"""
        self.clients.add(websocket)
        player_id = f"player_{len(self.player_data)}"
        self.player_data[player_id] = {
            "id": player_id,
            "x": 5,
            "y": 10,
            "inventory": {}
        }
        
        # 发送当前世界状态给新玩家
        await websocket.send(json.dumps({
            "type": "init",
            "player_id": player_id,
            "world_state": self.world_state,
            "players": self.player_data
        }))
        
        # 通知其他玩家有新玩家加入
        await self.broadcast({
            "type": "player_joined",
            "player_id": player_id,
            "player_data": self.player_data[player_id]
        }, exclude=websocket)
        
        return player_id

    async def unregister_client(self, websocket, player_id):
        """注销客户端"""
        self.clients.discard(websocket)
        if player_id in self.player_data:
            del self.player_data[player_id]
        
        await self.broadcast({
            "type": "player_left",
            "player_id": player_id
        })

    async def handle_message(self, message, websocket, player_id):
        """处理来自客户端的消息"""
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "move":
                # 玩家移动
                self.player_data[player_id]["x"] = data["x"]
                self.player_data[player_id]["y"] = data["y"]
                await self.broadcast({
                    "type": "player_moved",
                    "player_id": player_id,
                    "x": data["x"],
                    "y": data["y"]
                }, exclude=websocket)
                
            elif msg_type == "block_change":
                # 方块变化
                x, y, block_id = data["x"], data["y"], data["block_id"]
                key = f"{x},{y}"
                self.world_state[key] = block_id
                await self.broadcast({
                    "type": "block_changed",
                    "x": x,
                    "y": y,
                    "block_id": block_id
                }, exclude=websocket)
                
            elif msg_type == "chat":
                # 聊天消息
                await self.broadcast({
                    "type": "chat_message",
                    "player_id": player_id,
                    "message": data["message"],
                    "timestamp": datetime.now().isoformat()
                }, exclude=websocket)
                
            elif msg_type == "inventory_update":
                # 背包更新
                self.player_data[player_id]["inventory"] = data["inventory"]
                await self.broadcast({
                    "type": "inventory_updated",
                    "player_id": player_id,
                    "inventory": data["inventory"]
                }, exclude=websocket)
                
        except json.JSONDecodeError:
            print(f"Invalid JSON from {player_id}")

    async def broadcast(self, message, exclude=None):
        """广播消息给所有客户端"""
        msg_str = json.dumps(message)
        disconnected = set()
        
        for client in self.clients:
            if exclude and client == exclude:
                continue
            try:
                await client.send(msg_str)
            except websockets.exceptions.ConnectionClosed:
                disconnected.add(client)
        
        # 清理断开的连接
        for client in disconnected:
            self.clients.discard(client)

    async def handle_client(self, websocket, path):
        """处理单个客户端连接"""
        player_id = None
        try:
            player_id = await self.register_client(websocket)
            print(f"Player {player_id} connected. Total: {len(self.clients)}")
            
            async for message in websocket:
                await self.handle_message(message, websocket, player_id)
                
        except websockets.exceptions.ConnectionClosed:
            if player_id:
                await self.unregister_client(websocket, player_id)
                print(f"Player {player_id} disconnected. Total: {len(self.clients)}")

    async def start(self):
        """启动服务器"""
        print(f"Starting game server on {self.host}:{self.port}")
        async with websockets.serve(self.handle_client, self.host, self.port):
            await asyncio.Future()  # 无限运行

if __name__ == "__main__":
    import sys
    
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    server = GameServer(port=port)
    asyncio.run(server.start())
