#!/usr/bin/env python3
"""
Section 3, Problem 1 - Collaborative Document Editing using gRPC
gRPC Document Client

Usage:
    python client.py <host:port>
    python client.py localhost:50051

Provides an interactive CLI menu:
  1. Create Document
  2. Open Document
  3. Edit Document
  4. Subscribe to Updates
  5. Exit

Subscription runs in a background thread so the user can continue
interacting with the CLI while receiving live updates.
"""

import sys
import threading
import grpc
import document_pb2
import document_pb2_grpc


def subscribe_worker(stub, doc_name: str, stop_event: threading.Event):
    """
    Background thread that listens for document updates and prints them.
    Exits when stop_event is set or the stream ends.
    """
    request = document_pb2.UpdateRequest(name=doc_name)
    try:
        for update in stub.SubscribeToUpdates(request):
            if stop_event.is_set():
                break
            print(
                f"\n[Update] Document {update.name} modified.\n"
                f"{update.content}\n"
                f"> ", end="", flush=True
            )
    except grpc.RpcError as e:
        if not stop_event.is_set():
            print(f"\n[Subscription ended for '{doc_name}']: {e.details()}")


def run(address: str):
    channel = grpc.insecure_channel(address)
    stub = document_pb2_grpc.DocumentServiceStub(channel)

    # Track active subscriptions: {doc_name -> (thread, stop_event)}
    subscriptions: dict[str, tuple] = {}

    print(f"Connected to Document Server at {address}")
    print("=" * 50)

    while True:
        print("\n--- Menu ---")
        print("1. Create Document")
        print("2. Open Document")
        print("3. Edit Document")
        print("4. Subscribe to Updates")
        print("5. Exit")
        choice = input("> ").strip()

        if choice == "1":
            # ---- Create Document ----
            name = input("Document name: ").strip()
            content = input("Initial content: ").strip()
            if not name:
                print("[Client] Error: name cannot be empty.")
                continue
            try:
                resp = stub.CreateDocument(
                    document_pb2.CreateDocumentRequest(name=name, content=content)
                )
                if resp.success:
                    print(f"[Client] Document {name} created.")
                else:
                    print(f"[Client] Error: {resp.message}")
            except grpc.RpcError as e:
                print(f"[Client] RPC Error: {e.details()}")

        elif choice == "2":
            # ---- Open / Get Document ----
            name = input("Document name: ").strip()
            try:
                resp = stub.GetDocument(
                    document_pb2.GetDocumentRequest(name=name)
                )
                if resp.success:
                    print(f"[Client] {resp.content}")
                else:
                    print(f"[Client] Error: {resp.message}")
            except grpc.RpcError as e:
                print(f"[Client] RPC Error: {e.details()}")

        elif choice == "3":
            # ---- Edit Document ----
            name = input("Document name: ").strip()
            try:
                pos = int(input("Insert position: ").strip())
            except ValueError:
                print("[Client] Invalid position (must be integer).")
                continue
            text = input("Text to insert: ").strip()
            try:
                resp = stub.EditDocument(
                    document_pb2.EditDocumentRequest(
                        name=name, position=pos, text=text
                    )
                )
                if resp.success:
                    print(f"[Client] Document updated: {resp.new_content}")
                else:
                    print(f"[Client] Error: {resp.message}")
            except grpc.RpcError as e:
                print(f"[Client] RPC Error: {e.details()}")

        elif choice == "4":
            # ---- Subscribe to Updates ----
            name = input("Document name to subscribe: ").strip()
            if name in subscriptions:
                print(f"[Client] Already subscribed to '{name}'.")
                continue
            stop_event = threading.Event()
            t = threading.Thread(
                target=subscribe_worker,
                args=(stub, name, stop_event),
                daemon=True
            )
            t.start()
            subscriptions[name] = (t, stop_event)
            print(f"[Client] Subscribed to updates for '{name}'.")

        elif choice == "5":
            # ---- Exit ----
            print("[Client] Exiting.")
            # Stop all subscriptions
            for name, (t, ev) in subscriptions.items():
                ev.set()
            channel.close()
            break

        else:
            print("[Client] Unknown option. Please choose 1-5.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client.py <host:port>")
        print("Example: python client.py localhost:50051")
        sys.exit(1)
    run(sys.argv[1])
