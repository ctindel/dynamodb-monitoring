STACK_NAME=dynamodb-monitoring
export AWS_DEFAULT_REGION=us-east-2

aws cloudformation delete-stack --stack-name $STACK_NAME
