## Docker
- Build the Docker image and push to GitHub Container Registry 

```bash
docker build -t ghcr.io/websolutespa/ws-mark-flow:latest ./app
docker run --rm -d --name ws-mark-flow --env-file ./app/.env -p 8000:80 --add-host=host.docker.internal:host-gateway ghcr.io/websolutespa/ws-mark-flow:latest 
docker push ghcr.io/websolutespa/ws-mark-flow:latest
```