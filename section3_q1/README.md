# Section 3, Problem 1 — Collaborative Document Editing using gRPC

A multi-user collaborative document system built with gRPC and Python.

## Files

| File | Description |
|------|-------------|
| `document.proto` | Protocol Buffer service definition |
| `document_pb2.py` | Auto-generated proto message classes |
| `document_pb2_grpc.py` | Auto-generated gRPC service stubs |
| `server.py` | gRPC Document Server |
| `client.py` | Interactive gRPC Client CLI |

---

## Requirements

```bash
pip install grpcio grpcio-tools
```

Regenerate gRPC stubs (if you modify `document.proto`):

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. document.proto
```

---

## Running the System

### 1. Start the Server

```bash
python server.py localhost:50051
```

The server starts and listens on `localhost:50051`.

### 2. Start Client(s) (in separate terminals)

```bash
python client.py localhost:50051
```

Multiple clients can connect simultaneously.

---

## Client Menu Options

```
1. Create Document   — creates a named document with initial content
2. Open Document     — fetches and prints the current content
3. Edit Document     — inserts text at a character offset
4. Subscribe         — subscribes to live updates (background thread)
5. Exit
```

---

## Demo: Reproducing the PDF Example

### Terminal 0 — Server
```bash
python server.py localhost:50051
```

### Terminal 1 — Client 1
```
> 1
Document name: report.txt
Initial content: Hello World
[Client] Document report.txt created.

> 2
Document name: report.txt
[Client] Hello World

> 3
Document name: report.txt
Insert position: 6
Text to insert: Distributed 
[Client] Document updated: Hello Distributed World
```

### Terminal 2 — Client 2
```
> 2
Document name: report.txt
[Client] Hello World

> 4
Document name to subscribe: report.txt
[Client] Subscribed to updates for 'report.txt'.

# After Client 1 edits, Client 2 automatically receives:
[Update] Document report.txt modified.
Hello Distributed World
```

---

## Concurrent Edit Behaviour

When two clients edit the **same** document simultaneously:

```
Client 1: Edit report.txt at position 6 → insert "Distributed "
Client 2: Edit report.txt at position 6 → insert "Systems "
```

The server serializes concurrent edits using a **per-document lock**, ensuring no corruption. Edits to different documents are fully parallel.

> **Note:** Operational Transformation (OT) and CRDTs are **not** required; the server applies edits in the order they are received.

---

## Architecture

```
┌──────────────┐        gRPC (insecure)         ┌──────────────────────┐
│  Client 1    │ ──────────────────────────────► │                      │
└──────────────┘                                  │   Document Server    │
                                                  │   - In-memory docs   │
┌──────────────┐        gRPC (insecure)           │   - Per-doc locks    │
│  Client 2    │ ──────────────────────────────► │   - Subscriber queues│
└──────────────┘                                  └──────────────────────┘
                  ◄── stream DocumentUpdate ───
```

### Synchronization
- **Per-document `threading.Lock`**: Serializes `EditDocument` calls for the same document.
- **Global dict lock**: Protects the top-level documents/subscribers dictionaries.
- **Subscriber queues**: Each subscriber gets its own `queue.Queue`; the server `put_nowait`s updates after releasing the doc lock.
