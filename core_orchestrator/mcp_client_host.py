import os
import json
import asyncio
import sys
from contextlib import AsyncExitStack
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "mcp_config.json")

class SARModelContextManager:
    """
    Manages the lifecycle of multiple containerized MCP servers,
    utilizing an AsyncExitStack to handle context-managed stdio pipelines safely.
    """
    def __init__(self):
        self.sessions = {}
        self._exit_stack = None

    async def initialize_servers(self):
        """Reads configuration maps and connects to all registered MCP engines."""
        if not os.path.exists(CONFIG_PATH):
            print(f"❌ [MCP Host Error] Configuration mapping file missing at: {CONFIG_PATH}")
            return
            
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
            
        # Initialize the structural context manager stack
        self._exit_stack = AsyncExitStack()
            
        servers = config.get("mcpServers", {})
        for server_name, server_meta in servers.items():
            # Translate relative folder arg paths to match actual workspace naming configurations
            args = [
                arg.replace("mcp_servers/telemetry-server", "mcp_servers/telemetry_server")
                for arg in server_meta.get("args", [])
            ]
            
            server_params = StdioServerParameters(
                command=server_meta.get("command", "python"),
                args=args,
                env=os.environ.copy()
            )
            
            try:
                print(f"🔌 [MCP Connect] Bootstrapping subprocess pipe channel for: {server_name}...")
                
                # 🚀 THE CRITICAL FIX: Enter the stdio async context manager via ExitStack
                read_stream, write_stream = await self._exit_stack.enter_async_context(
                    stdio_client(server_params)
                )
                
                # Form the protocol tracking session link
                session = await self._exit_stack.enter_async_context(
                    ClientSession(read_stream, write_stream)
                )
                
                # Initialize JSON-RPC handshake sequences
                await session.initialize()
                
                self.sessions[server_name] = session
                print(f"✅ [MCP Connected] Server '{server_name}' initialized and holding active tool schemas.")
            except Exception as e:
                print(f"❌ [MCP Init Fault] Failed to spin up sub-server link [{server_name}]: {e}")

    async def call_mcp_tool(self, server_name: str, tool_name: str, arguments: dict):
        """Invokes a specific registered server schema action across the protocol pipe layer."""
        session = self.sessions.get(server_name)
        if not session:
            raise RuntimeError(f"Target MCP server '{server_name}' is not running or unmapped.")
            
        try:
            result = await session.call_tool(tool_name, arguments=arguments)
            # FastMCP tools return structural data inside .content list arrays
            if isinstance(result.content, list) and len(result.content) > 0:
                return result.content[0].text
            return result.content
        except Exception as e:
            print(f"❌ [MCP Call Error] Sub-process tool execution dropped on {server_name}::{tool_name}: {e}")
            raise e

    async def shutdown_servers(self):
        """Cleanly tears down all subprocess pipelines on container exit."""
        if self._exit_stack:
            await self._exit_stack.aclose()
            print("🔌 [MCP Cleanup] Closed all active sub-process pipe handles safely.")

# Instantiate single global tracking manager
mcp_host_manager = SARModelContextManager()