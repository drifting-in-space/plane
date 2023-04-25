#!/bin/sh
# shellcheck enable=all

set -x

export EXIT_TIMEOUT
export EXIT_CODE

trap 'exit ${EXIT_CODE}' TERM
[ -n "${EXIT_TIMEOUT}" ] && { sleep "${EXIT_TIMEOUT}" && kill -TERM "${PID}" & }

while true; do
	cat <<-EOF  | busybox nc -l -p 8080
	HTTP/1.1 200 OK
	Content-Type: text/html
	Connection: close

	<html><body><h1>Hello, World!</h1></body></html>'
	EOF
done 
