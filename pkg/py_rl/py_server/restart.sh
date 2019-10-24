#!/bin/bash
kubectl -n kube-system delete clusterrolebinding.rbac.authorization.k8s.io system:poseidon
kubectl -n kube-system delete clusterrole.rbac.authorization.k8s.io system:poseidon
kubectl -n kube-system delete serviceaccount poseidon
kubectl -n kube-system delete deployment.apps poseidon
kubectl -n kube-system delete service poseidon
kubectl -n kube-system delete serviceaccount heapster
kubectl -n kube-system delete clusterrolebinding.rbac.authorization.k8s.io heapster
kubectl -n kube-system delete deployment.apps heapster
kubectl -n kube-system delete service heapster
kubectl apply -f poseidon-deployment.yaml
kubectl apply -f heapster-poseidon.yaml