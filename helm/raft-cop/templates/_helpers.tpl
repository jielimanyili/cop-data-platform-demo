{{- define "raft-cop.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "raft-cop.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "raft-cop.labels" -}}
helm.sh/chart: {{ include "raft-cop.name" . }}-{{ .Chart.Version | replace "+" "_" }}
{{ include "raft-cop.selectorLabels" . }}
{{- end }}

{{- define "raft-cop.selectorLabels" -}}
app.kubernetes.io/name: {{ include "raft-cop.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
