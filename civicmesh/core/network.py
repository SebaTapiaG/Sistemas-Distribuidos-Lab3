"""
civicmesh.core.network
Capa de transporte asíncrono sobre TCP utilizando asyncio para comunicación entre peers.
"""

import asyncio
import logging
from typing import Callable, Coroutine, Dict, List, Optional, Tuple
from civicmesh.core.protocol import CivicMessage

logger = logging.getLogger("civicmesh.network")


class AsyncNetworkTransport:
    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host = host
        self.port = port
        self.server: Optional[asyncio.Server] = None
        self.message_handler: Optional[Callable[[CivicMessage], Coroutine]] = None
        self._is_running = False
        self._active_tasks: List[asyncio.Task] = []

    async def start(self, message_handler: Callable[[CivicMessage], Coroutine]) -> Tuple[str, int]:
        """Inicia el servidor TCP asíncrono para escuchar mensajes entrantes."""
        self.message_handler = message_handler
        self.server = await asyncio.start_server(
            self._handle_client_connection,
            self.host,
            self.port,
        )
        self._is_running = True
        
        # Obtener puerto asignado (especialmente si se pasó 0 para puerto aleatorio)
        sockets = self.server.sockets
        if sockets:
            self.host, self.port = sockets[0].getsockname()[:2]

        logger.info(f"Servidor de red iniciado en {self.host}:{self.port}")
        return self.host, self.port

    async def _handle_client_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        """Maneja el flujo de mensajes recibidos de una conexión TCP cliente."""
        peername = writer.get_extra_info("peername")
        try:
            while self._is_running:
                line = await reader.readline()
                if not line:
                    break
                line_str = line.decode("utf-8").strip()
                if not line_str:
                    continue

                try:
                    msg = CivicMessage.from_json(line_str)
                    if self.message_handler:
                        # Procesar de forma no bloqueante
                        task = asyncio.create_task(self.message_handler(msg))
                        self._active_tasks.append(task)
                        task.add_done_callback(lambda t: self._active_tasks.remove(t) if t in self._active_tasks else None)
                except Exception as e:
                    logger.warning(f"Error al deserializar mensaje desde {peername}: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"Conexión terminada con {peername}: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def send_message(self, target_host: str, target_port: int, msg: CivicMessage, timeout: float = 3.0) -> bool:
        """Envía un mensaje CivicMessage a un endpoint específico vía TCP."""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(target_host, target_port),
                timeout=timeout,
            )
            writer.write(msg.to_bytes())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return True
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
            logger.debug(f"Fallo al enviar mensaje a {target_host}:{target_port}: {e}")
            return False
        except Exception as e:
            logger.warning(f"Error inesperado al enviar a {target_host}:{target_port}: {e}")
            return False

    async def broadcast_message(
        self,
        targets: List[Tuple[str, int]],
        msg: CivicMessage,
        timeout: float = 3.0,
    ) -> int:
        """Envía el mensaje en paralelo a una lista de endpoints y retorna el número de envíos exitosos."""
        if not targets:
            return 0
        tasks = [self.send_message(h, p, msg, timeout=timeout) for h, p in targets]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(1 for r in results if r is True)

    async def stop(self):
        """Detiene el servidor y cancela tareas pendientes."""
        self._is_running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for task in list(self._active_tasks):
            if not task.done():
                task.cancel()
        logger.info(f"Servidor de red detenido en {self.host}:{self.port}")
