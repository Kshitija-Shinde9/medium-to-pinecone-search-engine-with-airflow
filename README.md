# Medium Article Search Engine
### Built with Apache Airflow + Pinecone | DATA226 Homework 10

**Student:** Kshitija Shinde 

**Course:** Data Warehouse - SJSU, Spring 2026 

**GitHub:** https://github.com/Kshitija-Shinde9/medium-to-pinecone-search-engine-with-airflow

---

## The Problem I Was Solving

Imagine you have thousands of Medium articles and you want to search through them. A normal keyword search would only find articles that contain the exact word you typed. But what if you type *"what is ethics in AI"* — you would want articles about AI ethics, bias in algorithms, fairness in machine learning — not just articles that literally contain those exact words.

That is what this project does. It understands the meaning behind your search, not just the keywords. This is called semantic search.

---

## How It Works — The Big Picture

![Pipeline Architecture](pipeline_diagram.png)

Every step runs automatically one after another inside Airflow. If one step fails, the rest stop. Each step logs exactly what it did so you can debug it.

---

## Step 1 — Modify docker-compose.yaml to include the required packages

Airflow runs inside Docker. Out of the box, it does not have Pinecone or sentence-transformers installed. So I added them to the `docker-compose.yaml` file:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: "sentence-transformers==3.1.1 pinecone==5.3.1 pandas requests"
```

I pinned the exact versions because newer versions sometimes break things. Then I restarted Docker:

```bash
docker compose down
docker compose up --build
```

The `--build` flag makes sure Docker picks up the new packages when it rebuilds the container.

**Screenshot — docker-compose.yaml showing the added packages in VS Code:**

<img width="3010" height="1294" alt="image" src="https://github.com/user-attachments/assets/37894829-ed6f-4c49-972d-b84d4b323364" />

---

**Screenshot — Terminal showing docker compose down:**

<img width="1760" height="288" alt="image" src="https://github.com/user-attachments/assets/d6d1d6c1-581e-44a4-93d0-80f0c2261e34" />

---

**Screenshot — Terminal showing docker compose up --build:**

<img width="1710" height="1112" alt="image" src="https://github.com/user-attachments/assets/6fc87ee6-bcd7-4b3d-ac63-14d741ca64e6" />
 
---

## Step 2 — Configure Pinecone and create the Airflow Variable

Pinecone is the database where all the article vectors get stored. I created a free account at pinecone.io and generated an API key.

Instead of hardcoding the API key in my Python code (which is a security risk), I saved it as an Airflow Variable. This way the key lives in Airflow's database, not in my code.

In Airflow UI: **Admin → Variables → + → Key:** `pinecone_api_key` → paste the key → Save

**Screenshot — Pinecone dashboard showing the API key was generated:**

<img width="1426" height="1296" alt="image" src="https://github.com/user-attachments/assets/df83ffb2-bcca-4e3f-82a5-f84ba4c474bf" />


---

**Screenshot — Airflow Edit Variable page with pinecone_api_key saved:**

<img width="3406" height="864" alt="image" src="https://github.com/user-attachments/assets/abd2999a-37f3-4ed4-8897-0c5bf34b2670" />

---

**Screenshot — Airflow List Variables page confirming the variable is saved:**

<img width="3390" height="584" alt="image" src="https://github.com/user-attachments/assets/9f9abcba-ad94-40dc-b7f3-64c0832a8c18" />

---

## Step 3 — Download and preprocess the data

The DAG downloads a CSV of Medium articles from an S3 bucket. The file had 2,499 lines (1 header + 2,498 articles).

Then it cleans the data:
- Fills in any empty titles or subtitles with blank strings so nothing crashes later
- Combines the title and subtitle into one field called metadata
- Assigns a unique ID number to each article

The cleaned file gets saved to `/tmp/medium_data/medium_preprocessed.csv` inside the Docker container.

**Screenshot — Airflow DAGs list showing Medium_to_Pinecone:**

<img width="3400" height="642" alt="image" src="https://github.com/user-attachments/assets/d1f8e7f9-064e-46ee-bf09-ce2c910ea695" />

---

**Screenshot — Airflow DAG graph view showing all 5 tasks:**

<img width="3410" height="1800" alt="image" src="https://github.com/user-attachments/assets/b72e87b2-dbff-4d04-92d4-62a6ada5ee9a" />

---

**Screenshot — Airflow logs for download_data task:**

<img width="3416" height="956" alt="image" src="https://github.com/user-attachments/assets/b7b38f33-3542-41a5-a894-37c1a11cb4f3" />

---

**Screenshot — Airflow logs for preprocess_data task:**

<img width="3402" height="906" alt="image" src="https://github.com/user-attachments/assets/fd18a7dd-e69d-4a2d-a6af-b52955c104a3" />


---

## Step 4 — Create the Pinecone index

Before storing anything in Pinecone, you need an index — think of it like creating a table in a database before you insert rows.

My code:
1. Checks if an index called `semantic-search-fast` already exists
2. If it does, deletes it so we always start fresh
3. Creates a new one with 384 dimensions, dotproduct metric, AWS us-east-1 region

**Screenshot — Airflow logs for create_pinecone_index task:**

<img width="3400" height="992" alt="image" src="https://github.com/user-attachments/assets/0390fc31-2917-4082-bc99-311dd9cc999b" />

---

**Screenshot — Pinecone dashboard showing the index was created:**

<img width="3414" height="1864" alt="image" src="https://github.com/user-attachments/assets/97a0fee3-0bce-46e6-bf23-2ff7079dbfb6" />

---

## Step 5 — Generate embeddings and upload to Pinecone

Each article title gets converted into a list of 384 numbers by an AI model called `all-MiniLM-L6-v2`. I processed articles in batches of 100 to avoid running out of memory. There were 25 batches total.

Each batch takes 100 article titles, runs them through the model, and uploads the result to Pinecone.

**Result: 2,498 records successfully upserted to Pinecone.**

**Screenshot — Airflow logs for generate_embeddings_and_upsert task:**

<img width="3406" height="1860" alt="image" src="https://github.com/user-attachments/assets/2cd522a9-0abb-49fa-b9e8-1d856ce8f10b" />

---

## Step 6 — Run search against Pinecone

The last task runs an actual search to prove everything works. It searches for "what is ethics in AI" and returns the top 5 most relevant articles by meaning.

| Rank | Score | Article |
|------|-------|---------|
| 1 | 0.749 | Ethics in AI: Potential Root Causes for Biased Algorithms |
| 2 | 0.748 | The ethical implications of AI in design |
| 3 | 0.748 | Ethical Considerations in Machine Learning Projects |
| 4 | 0.655 | Navigating the Ethical Contours of AI Copy Generation |
| 5 | 0.655 | Ethics in AI Copy Generation |

**Screenshot — Airflow logs for test_search_query task:**

<img width="3410" height="1296" alt="image" src="https://github.com/user-attachments/assets/7093ade3-2c1e-42fe-8519-174db0597890" />

---

**Screenshot — Final DAG run with all tasks showing success:**

<img width="3406" height="1806" alt="image" src="https://github.com/user-attachments/assets/14cff534-4173-447d-9259-2630457a6b50" />

---

## Files in This Repo

```
├── dags/
│   └── build_pinecone_search.py   <- The entire Airflow pipeline (5 tasks)
├── screenshots/                   <- All step-by-step screenshots
├── docker-compose.yaml            <- Docker setup with all packages included
├── pipeline_diagram.png           <- Architecture diagram
└── README.md
```

---

## Run It Yourself

### What you need
- Docker Desktop with at least 4GB RAM allocated
- A free Pinecone account at pinecone.io

### 1. Clone and start

```bash
git clone https://github.com/Kshitija-Shinde9/medium-to-pinecone-search-engine-with-airflow.git
cd medium-to-pinecone-search-engine-with-airflow

docker compose up --build
```

Wait about 60-90 seconds. Then go to http://localhost:8081  
Give Login & Password

### 2. Add your Pinecone API key

Admin -> Variables -> click +  
Key: `pinecone_api_key`  
Value: paste your key from pinecone.io -> API Keys  
Save.

### 3. Run the pipeline

Go to DAGs -> find `Medium_to_Pinecone` -> toggle it ON -> click the play button.

Watch all 5 tasks turn green one by one. Total runtime: roughly 5-10 minutes.

---

## Things That Were Tricky

**Package versions matter.** I used `sentence-transformers==3.1.1` and `pinecone==5.3.1` with pinned versions because the latest versions had breaking API changes. Without pinning, the pipeline would fail on startup.

**The AIRFLOW_UID warning.** When running on Mac, Docker shows a warning about AIRFLOW_UID not being set. This is harmless on Mac — it only matters on Linux where file permissions work differently.

**Memory and batching.** Loading all 2,498 articles and generating embeddings at once would crash on low-memory machines. Processing in batches of 100 keeps memory usage flat.

**XCom for passing data between tasks.** Airflow tasks do not share memory. The file path from `download_data` gets passed to `preprocess_data` through Airflow's XCom system — that is why you see "Returned value was: /tmp/medium_data/..." in the logs.

---

## Tech Stack

| Tool | What it does here |
|------|------------------|
| Apache Airflow 2.10.1 | Orchestrates all 5 pipeline tasks in order |
| Docker | Runs Airflow and Postgres without installing anything locally |
| PostgreSQL 13 | Airflow's internal database for DAG runs, logs, variables |
| Pinecone | Stores and searches the article vectors |
| sentence-transformers 3.1.1 | Converts article text into 384-dimensional vectors |
| all-MiniLM-L6-v2 | The specific AI model used for embeddings, runs on CPU |
| pandas | Reads and cleans the CSV file |
| requests | Downloads the CSV from S3 |
