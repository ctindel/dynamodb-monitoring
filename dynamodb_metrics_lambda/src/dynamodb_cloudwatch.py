import sys, os
here = os.path.dirname(os.path.realpath(__file__))
vendored_dir = os.path.join(here, 'vendored')
sys.path.append(vendored_dir)
import boto3
import json
import datetime

# Create CloudWatch client
cloudwatch = boto3.client('cloudwatch')
ddb = boto3.client('dynamodb')
aas = boto3.client('application-autoscaling')

# Constants
DEFAULT_DYNAMODB_TABLE_LIMIT = 256
FIVE_MINS_SECS = 300
# We can't use AWS/DynamoDB since its reserved
CLOUDWATCH_CUSTOM_NAMESPACE = "AWS_DynamoDB" 

# Globals
ddb_account_limits = None
ddb_tables = {}
ddb_total_provisioned_rcu = 0
ddb_total_provisioned_wcu = 0
ddb_total_consumed_rcu = 0
ddb_total_consumed_wcu = 0

def success_response(event, context):
    body = {
        "message": "Executed successfully",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

def load_dynamodb_limits(event, context):
    global ddb_account_limits
    ddb_account_limits = ddb.describe_limits()
    ddb_account_limits.pop('ResponseMetadata', None)

    # Since there's no way to query the max table limit we will allow them to override
    #  this with an environment variable for the lambda function
    if 'DYNAMODB_ACCOUNT_TABLE_LIMIT' in os.environ:
        ddb_account_limits['AccountMaxTables'] = os.environ['DYNAMODB_ACCOUNT_TABLE_LIMIT']
    else:
        ddb_account_limits['AccountMaxTables'] = DEFAULT_DYNAMODB_TABLE_LIMIT
    #print(ddb_account_limits)

def load_dynamodb_tables(event, context):
    global ddb_tables
    
    paginator = ddb.get_paginator('list_tables')
    for response in paginator.paginate():
        for table_name in response['TableNames']:
            #if table_name == 'dynamodb-speed-test-blog':
            #if table_name == 'bank':
            ddb_tables[table_name] = {}
#    print(ddb_tables)

def gather_dynamodb_consumption(event, context):
    global ddb_tables
    global ddb_total_provisioned_rcu
    global ddb_total_provisioned_wcu

    for table in ddb_tables.keys():
        response = ddb.describe_table(TableName=table)
        print(response)
        if response['Table']['TableStatus'] != 'ACTIVE':
            return
        
        # Older tables that existed before the on demand feature shipped might not have this field
        if 'BillingModeSummary' in response['Table'] and 'BillingMode' in response['Table']['BillingModeSummary']:
            ddb_tables[table]['BillingMode'] = response['Table']['BillingModeSummary']['BillingMode']
        else:
            ddb_tables[table]['BillingMode'] = "PROVISIONED"

        ddb_tables[table]['ProvisionedThroughput'] = response['Table']['ProvisionedThroughput']
        # We don't need this field and it messes up our object->json dump
        if 'LastIncreaseDateTime' in ddb_tables[table]['ProvisionedThroughput']:
            ddb_tables[table]['ProvisionedThroughput'].pop('LastIncreaseDateTime')
        if 'LastDecreaseDateTime' in ddb_tables[table]['ProvisionedThroughput']:
            ddb_tables[table]['ProvisionedThroughput'].pop('LastDecreaseDateTime')
        ddb_total_provisioned_rcu += ddb_tables[table]['ProvisionedThroughput']['ReadCapacityUnits']
        ddb_total_provisioned_wcu += ddb_tables[table]['ProvisionedThroughput']['WriteCapacityUnits']
        ddb_tables[table]['autoscaling'] = {'ReadCapacityUnits' : None, 'WriteCapacityUnits' : None}

    # Since we call describe scalable targets for all tables, we need to do these in multiple loops so the 
    #  dict structures are setup properly first
    for table in ddb_tables.keys():
        aas_paginator = aas.get_paginator('describe_scalable_targets')
        for aas_response in aas_paginator.paginate(ServiceNamespace='dynamodb', ResourceIds=list(map(lambda table: 'table/' + table, ddb_tables.keys()))):
            print(aas_response)
            for target in aas_response['ScalableTargets']:
                # Slice off the leading "table/" from the ResourceId
                aas_table_name = target['ResourceId'][len("table/"):]
                # Slice off the leading "dynamodb:table:" from the Scalable Dimension
                aas_scalable_dimension = target['ScalableDimension'][len("dynamodb:table:"):]
                ddb_tables[aas_table_name]['autoscaling'][aas_scalable_dimension] = {}
                ddb_tables[aas_table_name]['autoscaling'][aas_scalable_dimension]['min'] = target['MinCapacity']
                ddb_tables[aas_table_name]['autoscaling'][aas_scalable_dimension]['max'] = target['MaxCapacity']
                aas_policy_response = aas.describe_scaling_policies(
                    ServiceNamespace='dynamodb', ResourceId=target['ResourceId'], ScalableDimension=target['ScalableDimension'])
                #print(aas_policy_response)
                ddb_tables[aas_table_name]['autoscaling'][aas_scalable_dimension]['target'] = aas_policy_response['ScalingPolicies'][0]['TargetTrackingScalingPolicyConfiguration']['TargetValue']
        ddb_tables[table]['gsis'] = {}
        if 'GlobalSecondaryIndexes' in response['Table']:
            for gsi in response['Table']['GlobalSecondaryIndexes']:
                ddb_tables[table]['gsis'][gsi['IndexName']] = {}
                ddb_tables[table]['gsis'][gsi['IndexName']]['ProvisionedThroughput'] = gsi['ProvisionedThroughput']
                # We don't need this field and it messes up our object->json dump
                if 'LastIncreaseDateTime' in ddb_tables[table]['gsis'][gsi['IndexName']]['ProvisionedThroughput']:
                    ddb_tables[table]['gsis'][gsi['IndexName']]['ProvisionedThroughput'].pop('LastIncreaseDateTime')
                if 'LastDecreaseDateTime' in ddb_tables[table]['gsis'][gsi['IndexName']]['ProvisionedThroughput']:
                    ddb_tables[table]['gsis'][gsi['IndexName']]['ProvisionedThroughput'].pop('LastDecreaseDateTime')
                ddb_total_provisioned_rcu += ddb_tables[table]['gsis'][gsi['IndexName']]['ProvisionedThroughput']['ReadCapacityUnits']
                ddb_total_provisioned_wcu += ddb_tables[table]['gsis'][gsi['IndexName']]['ProvisionedThroughput']['WriteCapacityUnits']
                ddb_tables[table]['gsis'][gsi['IndexName']]['autoscaling'] = {'ReadCapacityUnits' : None, 'WriteCapacityUnits' : None}
                aas_paginator = aas.get_paginator('describe_scalable_targets')
                for aas_response in aas_paginator.paginate(ServiceNamespace='dynamodb', ResourceIds=list(map(lambda index: 'table/' + table + '/index/' + index, ddb_tables[table]['gsis'].keys()))):
                    for target in aas_response['ScalableTargets']:
                        # Slice off the leading "table/<table>/index/" from the ResourceId
                        aas_index_name = target['ResourceId'][len("table/" + table + "/index/"):]
                        # Slice off the leading "dynamodb:index:" from the Scalable Dimension
                        aas_scalable_dimension = target['ScalableDimension'][len("dynamodb:index:"):]
                        ddb_tables[table]['gsis'][aas_index_name]['autoscaling'][aas_scalable_dimension] = {}
                        ddb_tables[table]['gsis'][aas_index_name]['autoscaling'][aas_scalable_dimension]['min'] = target['MinCapacity']
                        ddb_tables[table]['gsis'][aas_index_name]['autoscaling'][aas_scalable_dimension]['max'] = target['MaxCapacity']
                        aas_policy_response = aas.describe_scaling_policies(
                            ServiceNamespace='dynamodb', ResourceId=target['ResourceId'], ScalableDimension=target['ScalableDimension'])
                        #print(aas_policy_response)
                        ddb_tables[table]['gsis'][aas_index_name]['autoscaling'][aas_scalable_dimension]['target'] = aas_policy_response['ScalingPolicies'][0]['TargetTrackingScalingPolicyConfiguration']['TargetValue']
                    print(aas_response)

def gather_dynamodb_metrics(event, context):
    global ddb_tables
    global ddb_total_consumed_rcu
    global ddb_total_consumed_wcu

    for table in ddb_tables.keys():
        ddb_tables[table]['metrics'] = {}
#        paginator = cloudwatch.get_paginator('list_metrics')
#        for response in paginator.paginate(Dimensions=[{'Name': 'TableName','Value': table}],
#                                           Namespace='AWS/DynamoDB'):
#            print(response['Metrics'])
        response = cloudwatch.get_metric_data(
            MetricDataQueries=[
                {
                    'Id' : 'consumed_rcu',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/DynamoDB',
                            'MetricName': 'ConsumedReadCapacityUnits',
                            'Dimensions': [{'Name': 'TableName', 'Value': table}]
                        },
                        'Period': FIVE_MINS_SECS,
                        'Stat': 'Average',
                        'Unit': 'Count'
                    },
                },
                {
                    'Id' : 'consumed_wcu',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/DynamoDB',
                            'MetricName': 'ConsumedWriteCapacityUnits',
                            'Dimensions': [{'Name': 'TableName', 'Value': table}]
                        },
                        'Period': FIVE_MINS_SECS,
                        'Stat': 'Average',
                        'Unit': 'Count'
                    }
                }
            ], 
            StartTime=datetime.datetime.now() - datetime.timedelta(minutes=15),
            EndTime=datetime.datetime.now(),
            MaxDatapoints=5
        )
        for result in response['MetricDataResults']:
            ddb_tables[table]['metrics'][result['Id']] = 0.0
            if len(result['Values']) > 0:
                ddb_tables[table]['metrics'][result['Id']] = result['Values'][0]
        #print(response)

def publish_dynamodb_account_metrics(event, context):
    global ddb_tables
    global ddb_account_limits

    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'AccountMaxReadCapacityUnits',
                'Unit': 'None',
                'Value': ddb_account_limits['AccountMaxReadCapacityUnits']
            }
        ],
        Namespace='AWS_DynamoDB'
    )

    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'AccountMaxWriteCapacityUnits',
                'Unit': 'None',
                'Value': ddb_account_limits['AccountMaxWriteCapacityUnits']
            }
        ],
        Namespace='AWS_DynamoDB'
    )

    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'TableMaxReadCapacityUnits',
                'Unit': 'None',
                'Value': ddb_account_limits['TableMaxReadCapacityUnits']
            }
        ],
        Namespace='AWS_DynamoDB'
    )

    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'TableMaxWriteCapacityUnits',
                'Unit': 'None',
                'Value': ddb_account_limits['TableMaxWriteCapacityUnits']
            }
        ],
        Namespace='AWS_DynamoDB'
    )

    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'ProvisionedReadCapacityUnitsAccountLimit',
                'Unit': 'Percent',
                'Value':  ddb_total_provisioned_rcu / ddb_account_limits['AccountMaxReadCapacityUnits']
            }
        ],
        Namespace='AWS_DynamoDB'
    )

    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'ProvisionedWriteCapacityUnitsAccountLimit',
                'Unit': 'Percent',
                'Value':  ddb_total_provisioned_wcu / ddb_account_limits['AccountMaxWriteCapacityUnits']
            }
        ],
        Namespace='AWS_DynamoDB'
    )

    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'AccountTableLimitPct',
                'Unit': 'Percent',
                'Value': len(ddb_tables.keys()) / ddb_account_limits['AccountMaxTables']
            }
        ],
        Namespace='AWS_DynamoDB'
    )

def publish_dynamodb_provisioned_table_metrics(table, event, context):
    global ddb_tables
    global ddb_account_limits

    if ddb_tables[table]['autoscaling']['ReadCapacityUnits'] is not None:
        cloudwatch.put_metric_data(
            MetricData=[
                {
                    'MetricName': 'ProvisionedReadCapacityAutoScalingPct',
                    'Dimensions': [{'Name': 'TableName', 'Value': table}],
                    'Unit': 'Percent',
                    'Value': ddb_tables[table]['ProvisionedThroughput']['ReadCapacityUnits'] / ddb_tables[table]['autoscaling']['ReadCapacityUnits']['max']
                }
            ],
            Namespace='AWS_DynamoDB'
        )

    if ddb_tables[table]['autoscaling']['WriteCapacityUnits'] is not None:
        cloudwatch.put_metric_data(
            MetricData=[
                {
                    'MetricName': 'ProvisionedWriteCapacityAutoScalingPct',
                    'Dimensions': [{'Name': 'TableName', 'Value': table}],
                    'Unit': 'Percent',
                    'Value': ddb_tables[table]['ProvisionedThroughput']['WriteCapacityUnits'] / ddb_tables[table]['autoscaling']['WriteCapacityUnits']['max']
                }
            ],
            Namespace='AWS_DynamoDB'
        )

    for gsi in ddb_tables[table]['gsis'].keys():
        if ddb_tables[table]['gsis'][gsi]['autoscaling']['ReadCapacityUnits'] is not None:
            cloudwatch.put_metric_data(
                MetricData=[
                    {
                        'MetricName': 'ProvisionedReadCapacityAutoScalingPct',
                        'Dimensions': [{'Name': 'GlobalSecondaryIndexName', 'Value': gsi}, {'Name': 'TableName', 'Value': table}],
                        'Unit': 'Percent',
                        'Value': ddb_tables[table]['gsis'][gsi]['ProvisionedThroughput']['ReadCapacityUnits'] / ddb_tables[table]['autoscaling']['ReadCapacityUnits']['max']
                    }
                ],
                Namespace='AWS_DynamoDB'
            )

        if ddb_tables[table]['gsis'][gsi]['autoscaling']['WriteCapacityUnits'] is not None:
            cloudwatch.put_metric_data(
                MetricData=[
                    {
                        'MetricName': 'ProvisionedWriteCapacityAutoScalingPct',
                        'Dimensions': [{'Name': 'GlobalSecondaryIndexName', 'Value': gsi}, {'Name': 'TableName', 'Value': table}],
                        'Unit': 'Percent',
                        'Value': ddb_tables[table]['gsis'][gsi]['ProvisionedThroughput']['WriteCapacityUnits'] / ddb_tables[table]['autoscaling']['WriteCapacityUnits']['max']
                    }
                ],
                Namespace='AWS_DynamoDB'
            )

def publish_dynamodb_ondemand_table_metrics(table, event, context):
    global ddb_tables
    global ddb_account_limits

    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'ConsumedReadCapacityTableLimitPct',
                'Dimensions': [{'Name': 'TableName', 'Value': table}],
                'Unit': 'Percent',
                'Value': ddb_tables[table]['ProvisionedThroughput']['ReadCapacityUnits'] / ddb_account_limits['TableMaxReadCapacityUnits']
            }
        ],
        Namespace='AWS_DynamoDB'
    )

    cloudwatch.put_metric_data(
        MetricData=[
            {
                'MetricName': 'ConsumedWriteCapacityTableLimitPct',
                'Dimensions': [{'Name': 'TableName', 'Value': table}],
                'Unit': 'Percent',
                'Value': ddb_tables[table]['ProvisionedThroughput']['WriteCapacityUnits'] / ddb_account_limits['TableMaxWriteCapacityUnits']
            }
        ],
        Namespace='AWS_DynamoDB'
    )

def publish_dynamodb_table_metrics(event, context):
    global ddb_tables
    global ddb_account_limits

    for table in ddb_tables.keys():
        if ddb_tables[table]['BillingMode'] == 'PROVISIONED':
            publish_dynamodb_provisioned_table_metrics(table, event, context)
        elif ddb_tables[table]['BillingMode'] == 'PAY_PER_REQUEST':
            publish_dynamodb_ondemand_table_metrics(table, event, context)
        else:
            raise Exception(f"Unknown billing mode {ddb_tables[table]['BillingMode']} for table {table}")

def publish_dynamodb_metrics(event, context):
    global ddb_tables
    global ddb_account_limits
    global ddb_total_provisioned_rcu
    global ddb_total_provisioned_wcu
    load_dynamodb_limits(event, context)
    load_dynamodb_tables(event, context)
    gather_dynamodb_consumption(event, context)
    gather_dynamodb_metrics(event, context)

    # can't use this because sometimes timestamps show up under ProvisionedThroughput.LastIncreaseDateTime
    print(json.dumps(ddb_tables, sort_keys=True, indent=4, separators=(',', ': ')))
    #print(ddb_tables)
    print(f"Using {len(ddb_tables.keys())} of max {ddb_account_limits['AccountMaxTables']} tables")
    print(f"DynamoDB AccountMaxReadCapacityUnits: {ddb_account_limits['AccountMaxReadCapacityUnits']}")
    print(f"DynamoDB AccountMaxWriteCapacityUnits: {ddb_account_limits['AccountMaxWriteCapacityUnits']}")
    print(f"DynamoDB TableMaxReadCapacityUnits: {ddb_account_limits['TableMaxReadCapacityUnits']}")
    print(f"DynamoDB TableMaxWriteCapacityUnits: {ddb_account_limits['TableMaxWriteCapacityUnits']}")
    print(f"DynamoDB Total Provisioned RCU: {ddb_total_provisioned_rcu}")
    print(f"DynamoDB Total Provisioned WCU: {ddb_total_provisioned_wcu}")

    publish_dynamodb_account_metrics(event, context)
    publish_dynamodb_table_metrics(event, context)

    return success_response(event, context)

if __name__ == "__main__":
    response = publish_dynamodb_metrics({}, {})
    print(f'{response}')
