"# Flam-Assignment---Backend(QueueCTL)" 

# queuectl — CLI-Based Background Job Queue System

> A Python-based background job queue system developed as part of a **technical assignment for Flam Company**.  
> It manages background jobs using worker processes, supports automatic retries with exponential backoff, and includes a Dead Letter Queue (DLQ) for permanently failed jobs — all through an intuitive CLI.

---

## Demo Video
 **Watch the Full Demo Here:**  
[https://drive.google.com/file/d/1Eiz7GD2bJCbzXHfA1Pze6eIAf6sWUWyO/view?usp=sharing](https://drive.google.com/file/d/1Eiz7GD2bJCbzXHfA1Pze6eIAf6sWUWyO/view?usp=sharing)

---

## Features
-  Enqueue and manage background jobs  
-  Run multiple worker processes concurrently  
-  Automatic retry with exponential backoff  
-  Dead Letter Queue (DLQ) for failed jobs  
-  Persistent SQLite storage (no data loss)  
-  Configurable retry limit and backoff base  
-  Simple, modular, and easy-to-use CLI  

---

## Tech Stack
| Component | Technology |
|------------|-------------|
| **Language** | Python 3.8+ |
| **Database** | SQLite |
| **Concurrency** | Multiprocessing |
| **CLI Parser** | argparse |
| **Logging** | Python `logging` module |

---

## Job Schema
```json
{
  "id": "unique-job-id",
  "command": "echo 'Hello World'",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "2025-11-08T10:00:00Z",
  "updated_at": "2025-11-08T10:00:00Z"
}

