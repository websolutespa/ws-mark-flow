## Docker
- Build the Docker image and push to GitHub Container Registry 

```bash
docker build -t ghcr.io/websolutespa/ws-mark-flow:latest ./app --label "org.opencontainers.image.source=https://github.com/websolutespa/ws-mark-flow"
docker push ghcr.io/websolutespa/ws-mark-flow:latest
```