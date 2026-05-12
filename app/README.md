## Docker
- Build the Docker image and push to GitHub Container Registry 

```bash
docker buildx inspect multiarch &>/dev/null \
  || docker buildx create --name multiarch

docker buildx use multiarch

docker buildx build --platform linux/amd64 \
  -t ghcr.io/websolutespa/ws-mark-flow:latest \
  --load --progress=plain ./app

docker push ghcr.io/websolutespa/ws-mark-flow:latest

docker run --rm -d --name ws-mark-flow --env-file ./app/.env -p 8000:80 --add-host=host.docker.internal:host-gateway ghcr.io/websolutespa/ws-mark-flow:latest 
```