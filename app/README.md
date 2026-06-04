## Docker
- Build the Docker image and push to GitHub Container Registry 

```bash
docker buildx inspect multiarch &>/dev/null \
  || docker buildx create --name multiarch

docker buildx use multiarch

docker buildx build --platform linux/amd64 \
  -t registry.websolute.dev/websolutespa/ws-mark-flow:latest \
  --push --progress=plain ./app

# Smaller image: skip baking Docling models into the image. The container will
# download them on first use. A persistent /opt/docling volume avoids repeated
# downloads after restarts, but is optional for environments without volumes.
docker buildx build --platform linux/amd64 \
  -t registry.websolute.dev/websolutespa/ws-mark-flow:latest \
  --build-arg DOCLING_PRELOAD_MODELS=0 \
  --push --progress=plain ./app

docker run --rm -d --name ws-mark-flow --env-file ./app/.env -p 8000:80 --add-host=host.docker.internal:host-gateway -v ws-mark-flow-docling:/opt/docling registry.websolute.dev/websolutespa/ws-mark-flow:latest

# No volume available: omit -v. Prefer the default build above, which bakes the
# models into the image. With DOCLING_PRELOAD_MODELS=0, models may download again
# when a new container starts.
docker run --rm -d --name ws-mark-flow --env-file ./app/.env -p 8000:80 --add-host=host.docker.internal:host-gateway registry.websolute.dev/websolutespa/ws-mark-flow:latest
```
