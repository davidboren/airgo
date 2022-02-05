#!/bin/bash

export ARGO_VERSION=${ARGO_VERSION:=v2.12.10}
export AE_VERSION=${AE_VERSION:=v0.11}
export K8_ENV=${K8_ENV:=eks-stage}
export RDS_ENDPOINT=${RDS_ENDPOINT}
export ROLE_ARN=${RDS_ENDPOINT}

echo "USING RDS_ENDPOINT=$RDS_ENDPOINT"
echo "USING ROLE_ARN=$ROLE_ARN"

NAMESPACE=argo-events

wget -q -O - https://raw.githubusercontent.com/argoproj/argo/$ARGO_VERSION/manifests/install.yaml | sed -E "s/namespace\:[[:space:]]+argo/namespace\: $NAMESPACE/g" | kubectl apply -f -

kubectl apply -n $NAMESPACE -f https://raw.githubusercontent.com/argoproj/argo-events/$AE_VERSION/hack/k8s/manifests/argo-events-sa.yaml
kubectl apply -n $NAMESPACE -f https://raw.githubusercontent.com/argoproj/argo-events/$AE_VERSION/hack/k8s/manifests/argo-events-cluster-roles.yaml
kubectl apply -n $NAMESPACE -f https://raw.githubusercontent.com/argoproj/argo-events/$AE_VERSION/hack/k8s/manifests/sensor-crd.yaml
kubectl apply -n $NAMESPACE -f https://raw.githubusercontent.com/argoproj/argo-events/$AE_VERSION/hack/k8s/manifests/gateway-crd.yaml

kubectl apply -n $NAMESPACE -f https://raw.githubusercontent.com/argoproj/argo-events/$AE_VERSION/hack/k8s/manifests/sensor-controller-configmap.yaml
wget -q -O - https://raw.githubusercontent.com/argoproj/argo-events/$AE_VERSION/hack/k8s/manifests/sensor-controller-deployment.yaml | sed -E "s/image\:[[:space:]]+argoproj\/sensor-controller/image\: argoproj\/sensor-controller:$AE_VERSION/g" | kubectl apply -n $NAMESPACE -f -

kubectl apply -n $NAMESPACE -f https://raw.githubusercontent.com/argoproj/argo-events/$AE_VERSION/hack/k8s/manifests/gateway-controller-configmap.yaml
wget -q -O - https://raw.githubusercontent.com/argoproj/argo-events/$AE_VERSION/hack/k8s/manifests/gateway-controller-deployment.yaml | sed -E "s/image\:[[:space:]]+argoproj\/gateway-controller/image\: argoproj\/gateway-controller:$AE_VERSION/g" | kubectl apply -n $NAMESPACE -f -

cat argo-overrides.yaml | sed -E "s/\\$\\{ROLE\_ARN\\}/$ROLE_ARN/g" | sed -E "s/\\$\\{ARGO\_VERSION\\}/$ARGO_VERSION/g" |  sed -E "s/\\$\\{RDS\_ENDPOINT\\}/$RDS_ENDPOINT/g" | kubectl apply -n $NAMESPACE -f -
kubectl apply -n $NAMESPACE -f roles.yaml
