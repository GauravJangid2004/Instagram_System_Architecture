[https://instagram-flow-explorer-vje2.arcada.app](https://instagram-flow-explorer-vje2.arcada.app)

# Instagram System Design

A system design for a large-scale Instagram-style social media platform. The design covers photo and video publishing, personalized feeds, social graph operations, engagement, stories, search, and notifications.

> **Interactive architecture:** [Open the live Instagram Flow Explorer](https://instagram-flow-explorer-vje2.arcada.app) to watch request packets move through the system blocks across different scenarios.
>
> **Full design document:** [Instagram System Design Document (PDF)](./Instagram_System_Design_Document.pdf) — place the PDF in the repository root for this link to work.

## Architecture at a Glance

The platform is organized into five layers. Synchronous requests handle user-facing operations, while Kafka and background workers take care of work that can finish after the API responds.

```mermaid
flowchart LR
    C[Mobile / Web Client]
    CDN[CDN]
    LB[GeoDNS + L7 Load Balancer]
    GW["API Gateway<br/>JWT auth + rate limits"]
    S["Stateless Services<br/>User · Post/Media · Feed · Graph<br/>Engagement · Story · Search · Notification"]
    PG[("PostgreSQL<br/>accounts + core metadata")]
    CA[("Cassandra<br/>likes + comments + expiring data")]
    R[("Redis Cluster<br/>feeds + cache + counters")]
    ES[("Elasticsearch<br/>search index")]
    S3[("S3 / Object Storage<br/>media + renditions")]
    K[Kafka Event Bus]
    W["Async Workers<br/>media · fan-out · notifications<br/>indexing · counters"]

    C -->|API requests| CDN
    CDN -->|API traffic| LB --> GW --> S
    C -->|media fetch| CDN
    CDN -->|cache miss| S3
    C -->|direct upload with pre-signed URL| S3
    S --> PG
    S --> CA
    S --> R
    S --> ES
    S -->|publish events| K --> W
    W --> PG
    W --> CA
    W --> R
    W --> ES
    W -->|processed media| S3
```

### Main Components

| Layer | Components | Responsibility |
| --- | --- | --- |
| Client | Mobile app, web app | Sends API calls, uploads media, and renders feeds and profiles. |
| Edge | CDN, GeoDNS, load balancer, API gateway | Delivers cached media, routes traffic, validates JWTs, and enforces rate limits. |
| Services | User, Post/Media, Feed, Graph, Engagement, Story, Search, Notification | Own focused product capabilities and coordinate data access. |
| Data | PostgreSQL, Cassandra, Redis, Elasticsearch, S3 | Stores relational records, high-volume activity, hot cache data, search indexes, and media. |
| Async processing | Kafka and worker pools | Buffers traffic spikes and decouples slow processing from API latency. |

## Request Flows

### 1. Upload a photo or video

Media bytes go directly from the client to object storage. The API acknowledges post creation before slower processing is complete.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant G as API Gateway
    participant P as Post/Media Service
    participant DB as PostgreSQL
    participant S3 as S3 / Object Storage
    participant K as Kafka
    participant M as Media Workers
    participant F as Fan-out Workers
    participant R as Redis Feeds
    participant X as Search / Notification Workers

    C->>G: POST /v1/media/upload-url
    G->>P: Authenticated request
    P-->>C: Pre-signed upload URL
    C->>S3: PUT media bytes directly
    C->>G: POST /v1/posts (media IDs + caption)
    G->>P: Create post
    P->>DB: Save post as PROCESSING
    P->>K: Publish post.created
    P-->>C: 202 Accepted
    par Background processing
        K->>M: Resize / transcode / moderate
        M->>DB: Mark post PUBLISHED
        M->>S3: Store media renditions
    and Feed delivery
        K->>F: Fan out post ID
        F->>R: Add post to follower feeds
    and Discovery and alerts
        K->>X: Index content and prepare notifications
    end
```

**Key behavior:** app servers do not carry large media uploads. Standard accounts use push fan-out after publishing; celebrity posts are merged into feeds at read time to avoid millions of write operations.

### 2. Open the home feed

The feed service reads precomputed post IDs, merges recent posts from high-follower accounts, ranks the results, then hydrates them with cached post and profile data.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant CDN as CDN
    participant LB as Load Balancer
    participant G as API Gateway
    participant F as Feed Service
    participant R as Redis
    participant DB as PostgreSQL / Cassandra
    participant S3 as S3 / Object Storage

    C->>LB: GET /v1/feed?limit=10&cursor=...
    LB->>G: Route request
    G->>G: Validate JWT + rate limit
    G->>F: Authenticated feed request
    F->>R: Read feed:{user_id} post IDs
    F->>F: Merge celebrity posts + rank
    F->>R: Batch-get posts, authors, counters
    opt Cache miss
        F->>DB: Fetch missing records
        DB-->>F: Post and author data
    end
    F-->>C: 200 OK + cursor + CDN media URLs
    C->>CDN: Fetch image/video
    opt CDN cache miss
        CDN->>S3: Fetch media object
        S3-->>CDN: Media bytes
    end
    CDN-->>C: Media bytes
```

### 3. Like a post

The like record is written idempotently and acknowledged quickly. Kafka consumers update counters and deliver notifications asynchronously.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant G as API Gateway
    participant E as Engagement Service
    participant CA as Cassandra
    participant K as Kafka
    participant A as Counter Aggregator
    participant R as Redis
    participant DB as PostgreSQL
    participant N as Notification Worker

    C->>G: POST /v1/posts/{id}/like
    G->>E: Authenticated request
    E->>CA: Idempotent like upsert
    E->>K: Publish like.created
    E-->>C: 200 OK (update UI)
    par Update counters
        K->>A: Consume like.created
        A->>R: Batch update like counter
        A->>DB: Persist denormalized count
    and Notify post author
        K->>N: Consume like.created
        N-->>N: Store alert + send APNs/FCM push
    end
```

### Other documented cases

- **Follow a public account:** update the follow graph and return `FOLLOWING`.
- **Follow a private account:** create a follow request and return `REQUESTED`; privacy checks use a strongly consistent source.
- **Search:** query Elasticsearch; indexing from new posts and users is asynchronous, so results can lag briefly.
- **Stories:** serve a story tray and expire story records after 24 hours using TTL-backed storage.
- **Comments:** persist the comment, return an acknowledgement, and process counters and notifications asynchronously.
- **Failures and fallbacks:** use a chronological feed if ranking is unavailable, rebuild or serve a stale feed on cache misses, and keep search/notifications isolated from core posting and feed paths.

## Data and Scaling Choices

| Data / workload | Store or approach | Reason |
| --- | --- | --- |
| Accounts, posts, and follow graph | PostgreSQL | Transactions, uniqueness constraints, and strong consistency for core records and privacy. |
| Likes and comments | Cassandra | High-volume, append-heavy access patterns partitioned by post or user. |
| Feeds, sessions, hot objects, counters | Redis | Low-latency reads and sorted sets for feed timelines. |
| Photos and videos | S3-compatible object storage + CDN | Durable, scalable media storage with edge delivery and direct uploads. |
| Search | Elasticsearch | Full-text and prefix search for users and hashtags. |
| Background work | Kafka + idempotent consumers | Buffers traffic spikes and decouples slow processing from API latency. |
| Feed fan-out | Hybrid push/pull | Push posts to regular followers; merge celebrity posts during feed reads. |

The design uses cursor pagination, read replicas, sharding, cache-aside behavior, and read-your-writes routing where needed. Accounts, credentials, and privacy operations favor strong consistency; feeds, counters, search, and notifications can converge asynchronously.

## Design Targets in the Document

These are assumptions and targets from the accompanying system design, not measurements of a production Instagram deployment.

- **Scale assumption:** 1 billion monthly active users and 500 million daily active users.
- **Feed API latency:** p99 below 200 ms; first image target below 1 second.
- **Availability target:** 99.99% for feed and read paths; 99.9% for uploads.
- **Traffic estimate:** about 250,000 average requests per second and 750,000 at peak.
- **Media strategy:** CDN delivery for more than 95% of media requests, with media uploaded directly to object storage.

## API Examples

The design proposes REST over HTTPS with JSON, JWT bearer authentication, idempotency keys for write retries, and cursor pagination for lists.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/v1/auth/signup`, `/v1/auth/login` | Account creation and login |
| `GET` | `/v1/users/{id}` | Profile |
| `POST` / `DELETE` | `/v1/users/{id}/follow` | Follow or unfollow |
| `POST` | `/v1/media/upload-url` | Request a pre-signed media upload URL |
| `POST` | `/v1/posts` | Create a post |
| `GET` | `/v1/feed` | Read a paginated home feed |
| `POST` / `DELETE` | `/v1/posts/{id}/like` | Like or unlike a post |
| `POST` / `GET` | `/v1/posts/{id}/comments` | Add or list comments |
| `GET` | `/v1/search` | Search users or tags (`type=user` or `type=tag`) |
| `GET` | `/v1/notifications` | List notifications |

## Repository Contents

```text
.
├── README.md
└── Instagram_System_Design_Document.pdf
```

Keep the PDF beside this README in the repository root, or update the relative PDF link above if you store it in a folder such as `docs/`.

## Contributors

- Anmol Sahu
- Gaurav Jangid
- Maulik Sharma
- Suryaansh Sharma

Prepared for the System Design and DevOps assignment at the Institute of Engineering and Technology, JK Lakshmipat University, Jaipur.
