STACK_NAME=dynamodb-monitoring
export AWS_DEFAULT_REGION=us-east-2

if ! aws cloudformation describe-stacks --stack-name $STACK_NAME > /dev/null 2>&1; then
    aws cloudformation create-stack --stack-name $STACK_NAME --template-body file://`pwd`/dynamodb_metrics_cf.yaml --parameters file://`pwd`/dynamodb_metrics_cf_params.json --capabilities CAPABILITY_IAM
else
    aws cloudformation update-stack --stack-name $STACK_NAME --template-body file://`pwd`/dynamodb_metrics_cf.yaml --parameters file://`pwd`/dynamodb_metrics_cf_params.json --capabilities CAPABILITY_IAM
fi
