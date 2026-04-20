# How to Run — Codeace Intelligent Data Onboarding Platform

---

## Step 1 — Install Ollama

Download and install Ollama for your operating system from the official site:

```
https://ollama.com/download
```

Follow the installer instructions for your platform (macOS, Windows, or Linux).

---

## Step 2 — Pull the Required Model

Once Ollama is installed, open a terminal and pull the `qwen2.5:7b` model:

```bash
ollama pull qwen2.5:7b
```

This downloads the model weights locally. The download is approximately 4.7 GB, so allow some time depending on your connection speed.

---

## Step 3 — Start the Ollama Server

In the same terminal (or a new one), start the Ollama server:

```bash
ollama serve
```

Keep this terminal open and running in the background. The server listens on `http://localhost:11434` by default.

> **Note:** On macOS, Ollama may already be running as a background service after installation. If `ollama serve` reports that the address is already in use, you can skip this step — the server is already active.

---

## Step 4 — Run With Docker Compose

Open a new terminal and navigate to the `machine_task/` directory:

```bash
cd machine_task
```

Build and start all backend services (Redis, FastAPI API, and Celery worker):

```bash
docker compose up --build
```

Wait until you see log output confirming all three services are healthy and running before proceeding.

---

## Step 5 — Start the Frontend

Open another new terminal and start the React frontend:

```bash
cd frontend
npm install
npm run dev
```

---

## Step 6 — Open the App

Once both the backend and frontend are running, open your browser and navigate to:

```
http://localhost:5173
```

You should see the Codeace platform ready to accept a CSV upload.

---

## Quick Reference — All Steps

| Step | Command |
|------|---------|
| Install Ollama | Download from `https://ollama.com/download` |
| Pull model | `ollama pull qwen2.5:7b` |
| Start Ollama | `ollama serve` |
| Start backend | `docker compose up --build` (from `machine_task/`) |
| Install frontend deps | `cd frontend && npm install` |
| Start frontend | `npm run dev` |
| Open app | `http://localhost:5173` |