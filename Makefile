.PHONY: up down demo test local local-demo loadtest loadtest-steady check aws-up aws-demo aws-check aws-down

up:              ; docker compose up --build -d
down:            ; docker compose down -v
demo:            ; docker compose --profile demo up --build
test:            ; python -m pytest -q
local:           ; python scripts/stackctl.py up
local-demo:      ; python -m loadtest.run --mode demo --couriers 45 --demo-interval 0.7
loadtest-steady: ; python scripts/stackctl.py up && python scripts/stackctl.py reset && python scripts/stackctl.py up && python -m loadtest.run --orders 2500 --rate 15 --couriers 150
loadtest:        ; python scripts/stackctl.py up && python scripts/stackctl.py reset && python scripts/stackctl.py up && python -m loadtest.run --orders 4000 --rate 15 --couriers 150 --chaos
check:           ; python scripts/aws_check.py

# Same stack, but the worker uses real AWS SQS (fill in .env.aws first)
aws-up:          ; docker compose --env-file .env.aws -f docker-compose.yml -f docker-compose.aws.yml up --build -d
aws-demo:        ; docker compose --env-file .env.aws -f docker-compose.yml -f docker-compose.aws.yml --profile demo up --build -d
aws-check:       ; docker compose --env-file .env.aws -f docker-compose.yml -f docker-compose.aws.yml exec worker python scripts/aws_check.py
aws-down:        ; docker compose --env-file .env.aws -f docker-compose.yml -f docker-compose.aws.yml down
