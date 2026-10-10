# COP Data Platform — Kubernetes Deployment

Deploys the COP Data Platform (Zookeeper + Kafka + the Java/Kotlin pipelines + monitoring) onto a local Kubernetes cluster. Part of ADR-004: turns "Kubernetes" from a gap into a hands-on, defensible claim.

## Prerequisites

```bash
brew install kubectl helm colima
```

## 1. Bring up the cluster

```bash
colima start --kubernetes       # single-node cluster (Lima VM)
kubectl config use-context colima
kubectl get nodes
```

## 2. Deploy Zookeeper + Kafka (plain manifests)

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/zookeeper.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/kafka-setup-job.yaml
```

Watch readiness:

```bash
kubectl -n cop get pods -w
```

## 3. Build + deploy the COP pipelines (Helm)

```bash
# build the two pipeline images into Colima's runtime
docker build -t cop-java-pipeline:latest -f java-pipeline/Dockerfile .
docker build -t cop-kotlin-pipeline:latest -f kotlin-pipeline/Dockerfile .

helm install cop-java ./helm/raft-cop \
  --namespace cop \
  --set image.repository=cop-java-pipeline \
  --set image.tag=latest \
  --set env.KAFKA_BOOTSTRAP_SERVERS=kafka:9092

helm install cop-kotlin ./helm/raft-cop \
  --namespace cop \
  --set image.repository=cop-kotlin-pipeline \
  --set image.tag=latest \
  --set env.STREAMS_APPLICATION_ID=cop-risk-scoring \
  --set env.KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

## 4. Monitoring (optional)

Deploy Prometheus + Grafana (reuse `monitoring/` configs) with the `prometheus-community` chart, or keep the Docker Compose monitoring stack.

## Administration demos (the "show me" list)

```bash
kubectl -n cop get all                      # overview
kubectl -n cop describe pod <name>          # events, probes, resources
kubectl -n cop logs -f <pod>                # streaming logs
kubectl -n cop scale deploy cop-java --replicas=3   # scale
kubectl -n cop rollout status deploy/cop-java
kubectl -n cop rollout undo deploy/cop-java          # rollback
kubectl -n cop port-forward svc/cop-java 8080:8080   # access locally
kubectl -n cop exec -it <kafka-pod> -- bash          # exec into kafka
```

## Teardown

```bash
kubectl delete namespace cop
colima stop
```

## Gotchas (hit and fixed during this setup)

1. **K8s auto-injects `KAFKA_PORT` from the `kafka` Service** — the Confluent `cp-kafka` image exits if `KAFKA_PORT` is set ("port is deprecated. Please use KAFKA_ADVERTISED_LISTENERS instead."). Fix: `enableServiceLinks: false` on the pod spec.
2. **Confluent `cp-zookeeper` disables `ruok`** (only `srvr` is enabled by default) — the original exec readiness probe failed. Fix: use `tcpSocket` probes.
3. **Multi-module Maven reactor** — the original Dockerfiles used `mvn -pl <module> -am`, which fails because the parent `pom.xml` lists sibling modules that aren't copied into the build context. Fix: `mvn -f <module>/pom.xml package`.

## Honesty boundary

This demonstrates **local cluster administration** (Deployments, Services, ConfigMaps, Jobs, probes, resources, Helm, scaling, rolling updates). It is **not** production multi-node ops, managed K8s (EKS/GKE/AKS), or Strimzi/Kafka-operator administration — those use an operator + multi-replica and are a separate level of maturity.
