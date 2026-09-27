#!/usr/bin/env python3
"""
Section 3, Problem 1 - Collaborative Document Editing using gRPC
gRPC Document Server

Usage:
    python server.py <host:port>
    python server.py localhost:50051

Features:
  - Maintain documents in memory
  - Handle requests from multiple clients (thread-safe)
  - Create and retrieve documents
  - Apply edits (text insertion at a specified position)
  - Maintain subscriber lists per document
  - Stream live updates to all subscribed clients via SubscribeToUpdates
  - Synchronize concurrent edits with a per-document lock
"""

import sys
import time
import queue
import threading
import concurrent.futures
import logging

import grpc
import document_pb2
import document_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SERVER] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


class DocumentServiceServicer(document_pb2_grpc.DocumentServiceServicer):
    """
    Implements the DocumentService gRPC service.

    Thread-safety design:
      - `_global_lock` protects the top-level `_documents` and
        `_subscribers` dictionaries (metadata access).
      - Each document has its own `threading.Lock` stored in `_doc_locks`.
        Edits acquire the per-document lock so two simultaneous edits to
        the *same* document are serialized, while edits to *different*
        documents proceed in parallel.
    """

    def __init__(self):
        # {name: str -> content: str}
        self._documents: dict[str, str] = {}
        # {name: str -> [queue.Queue, ...]}   one queue per subscriber
        self._subscribers: dict[str, list[queue.Queue]] = {}
        # {name: str -> threading.Lock}
        self._doc_locks: dict[str, threading.Lock] = {}
        # Global lock protecting dict structure (not doc content)
        self._global_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Helper: get or create the per-document lock
    # ------------------------------------------------------------------
    def _get_doc_lock(self, name: str) -> threading.Lock:
        with self._global_lock:
            if name not in self._doc_locks:
                self._doc_locks[name] = threading.Lock()
            return self._doc_locks[name]

    # ------------------------------------------------------------------
    # Helper: broadcast updated content to all subscribers of a document
    # ------------------------------------------------------------------
    def _broadcast(self, name: str, content: str):
        with self._global_lock:
            subscriber_queues = list(self._subscribers.get(name, []))

        update = document_pb2.DocumentUpdate(name=name, content=content)
        dead_queues = []
        for q in subscriber_queues:
            try:
                q.put_nowait(update)
            except queue.Full:
                dead_queues.append(q)

        # Clean up overflowed subscriber queues
        if dead_queues:
            with self._global_lock:
                subs = self._subscribers.get(name, [])
                for dq in dead_queues:
                    if dq in subs:
                        subs.remove(dq)

    # ------------------------------------------------------------------
    # RPC: CreateDocument
    # ------------------------------------------------------------------
    def CreateDocument(self, request, context):
        name = request.name.strip()
        content = request.content

        if not name:
            return document_pb2.CreateDocumentResponse(
                success=False,
                message="Document name cannot be empty."
            )

        with self._global_lock:
            if name in self._documents:
                return document_pb2.CreateDocumentResponse(
                    success=False,
                    message=f"Document '{name}' already exists."
                )
            self._documents[name] = content
            self._subscribers[name] = []
            self._doc_locks[name] = threading.Lock()

        logger.info(f"Created document '{name}' (len={len(content)})")
        return document_pb2.CreateDocumentResponse(
            success=True,
            message=f"Document {name} created."
        )

    # ------------------------------------------------------------------
    # RPC: GetDocument
    # ------------------------------------------------------------------
    def GetDocument(self, request, context):
        name = request.name.strip()

        with self._global_lock:
            if name not in self._documents:
                return document_pb2.GetDocumentResponse(
                    success=False,
                    content="",
                    message=f"Document '{name}' not found."
                )
            content = self._documents[name]

        logger.info(f"GetDocument '{name}'")
        return document_pb2.GetDocumentResponse(
            success=True,
            content=content,
            message=""
        )

    # ------------------------------------------------------------------
    # RPC: EditDocument
    # ------------------------------------------------------------------
    def EditDocument(self, request, context):
        name = request.name.strip()
        position = request.position
        text = request.text

        doc_lock = self._get_doc_lock(name)

        with doc_lock:
            with self._global_lock:
                if name not in self._documents:
                    return document_pb2.EditDocumentResponse(
                        success=False,
                        new_content="",
                        message=f"Document '{name}' not found."
                    )
                current = self._documents[name]

            # Clamp position to valid range
            position = max(0, min(position, len(current)))
            new_content = current[:position] + text + current[position:]

            with self._global_lock:
                self._documents[name] = new_content

        logger.info(
            f"EditDocument '{name}': inserted {len(text)} chars at pos {position}"
        )

        # Broadcast update OUTSIDE the doc lock to avoid blocking new edits
        self._broadcast(name, new_content)

        return document_pb2.EditDocumentResponse(
            success=True,
            new_content=new_content,
            message=""
        )

    # ------------------------------------------------------------------
    # RPC: SubscribeToUpdates (server-streaming)
    # ------------------------------------------------------------------
    def SubscribeToUpdates(self, request, context):
        name = request.name.strip()

        with self._global_lock:
            if name not in self._documents:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    f"Document '{name}' not found."
                )
                return

        # Create a per-subscriber queue
        subscriber_queue: queue.Queue = queue.Queue(maxsize=100)

        with self._global_lock:
            self._subscribers[name].append(subscriber_queue)

        logger.info(f"Client subscribed to '{name}'")

        try:
            while context.is_active():
                try:
                    update = subscriber_queue.get(timeout=1.0)
                    yield update
                except queue.Empty:
                    # Periodically check if client is still alive
                    continue
        finally:
            # Remove subscriber queue on disconnect
            with self._global_lock:
                if name in self._subscribers:
                    try:
                        self._subscribers[name].remove(subscriber_queue)
                    except ValueError:
                        pass
            logger.info(f"Client unsubscribed from '{name}'")


def serve(address: str):
    server = grpc.server(
        concurrent.futures.ThreadPoolExecutor(max_workers=20),
        options=[
            ('grpc.max_send_message_length', 50 * 1024 * 1024),
            ('grpc.max_receive_message_length', 50 * 1024 * 1024),
        ]
    )
    document_pb2_grpc.add_DocumentServiceServicer_to_server(
        DocumentServiceServicer(), server
    )
    server.add_insecure_port(address)
    server.start()
    logger.info(f"Document Server started on {address}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        server.stop(grace=5)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python server.py <host:port>")
        print("Example: python server.py localhost:50051")
        sys.exit(1)
    serve(sys.argv[1])
