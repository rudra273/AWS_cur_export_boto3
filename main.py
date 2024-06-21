import boto3
import datetime
import json
import os
from dotenv import load_dotenv

load_dotenv()

current_date = datetime.datetime.now().strftime('%Y-%m-%d')

def create_boto3_client(access_key, secret_key, region):
    session = boto3.Session( 
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    return session.client('bcm-data-exports')


def create_s3_bucket(bucket_name, region, access_key, secret_key):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    s3_client = session.client('s3', region_name=region)

    try:
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        print(f'Bucket {bucket_name} created successfully.')
        return True
    except Exception as e:
        print(f'Error creating bucket: {e}')
    


def get_aws_account_id(access_key, secret_key, region):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    sts_client = session.client('sts')
    try:
        identity_info = sts_client.get_caller_identity()
        account_id = identity_info['Account']
        return account_id
    except Exception as e:
        print(f'Error retrieving account ID: {e}')
        return None
    

def add_bucket_policy(bucket_name, account_id, access_key, secret_key, region):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    s3_client = session.client('s3')

    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "EnableAWSDataExportsToWriteToS3AndCheckPolicy",
                "Effect": "Allow",
                "Principal": {
                    "Service": [
                        "billingreports.amazonaws.com",
                        "bcm-data-exports.amazonaws.com"
                    ]
                },
                "Action": [
                    "s3:PutObject",
                    "s3:GetBucketPolicy"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*"
                ],
                "Condition": {
                    "StringLike": {
                        "aws:SourceAccount": account_id,
                        "aws:SourceArn": [
                            f"arn:aws:cur:{region}:{account_id}:definition/*",
                            f"arn:aws:bcm-data-exports:{region}:{account_id}:export/*"
                        ]
                    }
                }
            }
        ]
    }

    bucket_policy_string = json.dumps(bucket_policy)

    try:
        s3_client.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy_string)
        print(f'Policy added to bucket {bucket_name} successfully.')
    except Exception as e:
        print(f'Error adding bucket policy: {e}')



def create_export(client, export_name, export_description, s3_bucket, s3_prefix, s3_region, query_columns, start_date=None, end_date=None):
    columns = ', '.join(query_columns)
    query_statement = f"SELECT {columns} FROM COST_AND_USAGE_REPORT"

    response = client.create_export(
        Export={
            'Name': export_name,
            'Description': export_description,
            'DataQuery': {
                'QueryStatement': query_statement,
                'TableConfigurations': {
                    'COST_AND_USAGE_REPORT': {
                        'TIME_GRANULARITY': 'DAILY',
                        'INCLUDE_RESOURCES': 'FALSE',
                        'INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY': 'FALSE',
                        'INCLUDE_SPLIT_COST_ALLOCATION_DATA': 'FALSE'
                    }
                }
            },
            'DestinationConfigurations': {
                'S3Destination': {
                    'S3Bucket': s3_bucket,
                    'S3Prefix': f'{s3_prefix}/{current_date}/', 
                    'S3Region': s3_region,
                    'S3OutputConfigurations': {
                        'Overwrite': 'OVERWRITE_REPORT',
                        'Format': 'PARQUET',
                        'Compression': 'PARQUET',
                        'OutputType': 'CUSTOM'
                    }
                }
            },
            'RefreshCadence': {
                'Frequency': 'SYNCHRONOUS'
            }
        }
    )

    print(response)
    export_arn = response['ExportArn']
    with open('export_arn.json', 'w') as f:
        json.dump({'export_arn': export_arn}, f)
    print("Export ARN stored successfully.")

def get_export_arn_from_file():
    try:
        with open('export_arn.json', 'r') as f:
            data = json.load(f)
            return data.get('export_arn')
    except FileNotFoundError:
        print("Export ARN file not found.")
        return None

def update_export(client, export_name, export_description, s3_bucket, s3_prefix, s3_region, query_columns, start_date=None, end_date=None):
    columns = ', '.join(query_columns)
    # query_statement = f"""
    # SELECT {columns}
    # FROM COST_AND_USAGE_REPORT
    # WHERE line_item_usage_start_date BETWEEN '{start_date}' AND '{end_date}'
    # """
    query_statement = f"""
    SELECT {columns}
    FROM COST_AND_USAGE_REPORT
    """
    
    export_arn = get_export_arn_from_file()
    if export_arn:
        response = client.update_export(
            ExportArn=export_arn,
            Export={
            'Name': export_name,
            'Description': export_description,
            'DataQuery': {
                'QueryStatement': query_statement,
                'TableConfigurations': {
                    'COST_AND_USAGE_REPORT': {
                        'TIME_GRANULARITY': 'DAILY',
                        'INCLUDE_RESOURCES': 'FALSE',
                        'INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY': 'FALSE',
                        'INCLUDE_SPLIT_COST_ALLOCATION_DATA': 'FALSE'
                    }
                }
            },
            'DestinationConfigurations': {
                'S3Destination': {
                    'S3Bucket': s3_bucket,
                    'S3Prefix': f'{s3_prefix}/{current_date}/', 
                    'S3Region': s3_region,
                    'S3OutputConfigurations': {
                        'Overwrite': 'OVERWRITE_REPORT',
                        'Format': 'PARQUET',
                        'Compression': 'PARQUET',
                        'OutputType': 'CUSTOM'
                    }
                }
            },
            'RefreshCadence': {
                'Frequency': 'SYNCHRONOUS'
            }
        }
        )
        print(response)
        print('updated')
    else:
        print("Export ARN not found. Cannot update export.")

def bucket(S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    s3_client = session.client('s3')

    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        bucket_exists = True
    except:
        bucket_exists = False

    if not bucket_exists:
        create_s3_bucket(S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        account_id = get_aws_account_id(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
        add_bucket_policy(S3_BUCKET, account_id, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)



def main(bucket_name, bucket_prefix, export_name, export_desc):
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION')
    S3_BUCKET = bucket_name
    S3_PREFIX = bucket_prefix
    S3_REGION = os.getenv('S3_REGION')

    EXPORT_NAME = export_name 
    EXPORT_DESCRIPTION = export_desc

    start_date = '2024-01-01'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d') 

    query_columns = [
        'bill_bill_type', 'bill_billing_entity', 'bill_billing_period_end_date', 
        'bill_billing_period_start_date', 'bill_invoice_id', 'bill_invoicing_entity', 
        'bill_payer_account_id', 'bill_payer_account_name', 'cost_category', 'discount', 
        'discount_bundled_discount', 'discount_total_discount', 'identity_line_item_id', 
        'identity_time_interval', 'line_item_availability_zone', 'line_item_blended_cost', 
        'line_item_blended_rate', 'line_item_currency_code', 'line_item_legal_entity', 
        'line_item_line_item_description', 'line_item_line_item_type', 'line_item_net_unblended_cost', 
        'line_item_net_unblended_rate', 'line_item_normalization_factor', 'line_item_normalized_usage_amount', 
        'line_item_operation', 'line_item_product_code', 'line_item_tax_type', 'line_item_unblended_cost', 
        'line_item_unblended_rate', 'line_item_usage_account_id', 'line_item_usage_account_name', 
        'line_item_usage_amount', 'line_item_usage_end_date', 'line_item_usage_start_date', 
        'line_item_usage_type', 'pricing_currency', 'pricing_lease_contract_length', 'pricing_offering_class', 
        'pricing_public_on_demand_cost', 'pricing_public_on_demand_rate', 'pricing_purchase_option', 
        'pricing_rate_code', 'pricing_rate_id', 'pricing_term', 'pricing_unit', 'product', 
        'product_comment', 'product_fee_code', 'product_fee_description', 'product_from_location', 
        'product_from_location_type', 'product_from_region_code', 'product_instance_family', 
        'product_instance_type', 'product_instancesku', 'product_location', 'product_location_type', 
        'product_operation', 'product_pricing_unit', 'product_product_family', 'product_region_code', 
        'product_servicecode', 'product_sku', 'product_to_location', 'product_to_location_type', 
        'product_to_region_code', 'product_usagetype', 'reservation_amortized_upfront_cost_for_usage', 
        'reservation_amortized_upfront_fee_for_billing_period', 'reservation_availability_zone', 
        'reservation_effective_cost', 'reservation_end_time', 'reservation_modification_status', 
        'reservation_net_amortized_upfront_cost_for_usage', 'reservation_net_amortized_upfront_fee_for_billing_period', 
        'reservation_net_effective_cost', 'reservation_net_recurring_fee_for_usage', 
        'reservation_net_unused_amortized_upfront_fee_for_billing_period', 'reservation_net_unused_recurring_fee', 
        'reservation_net_upfront_value', 'reservation_normalized_units_per_reservation', 
        'reservation_number_of_reservations', 'reservation_recurring_fee_for_usage', 
        'reservation_reservation_a_r_n', 'reservation_start_time', 'reservation_subscription_id', 
        'reservation_total_reserved_normalized_units', 'reservation_total_reserved_units', 
        'reservation_units_per_reservation', 'reservation_unused_amortized_upfront_fee_for_billing_period', 
        'reservation_unused_normalized_unit_quantity', 'reservation_unused_quantity', 
        'reservation_unused_recurring_fee', 'reservation_upfront_value', 'resource_tags', 
        'savings_plan_amortized_upfront_commitment_for_billing_period', 'savings_plan_end_time', 
        'savings_plan_instance_type_family', 'savings_plan_net_amortized_upfront_commitment_for_billing_period', 
        'savings_plan_net_recurring_commitment_for_billing_period', 'savings_plan_net_savings_plan_effective_cost', 
        'savings_plan_offering_type', 'savings_plan_payment_option', 'savings_plan_purchase_term', 
        'savings_plan_recurring_commitment_for_billing_period', 'savings_plan_region', 
        'savings_plan_savings_plan_a_r_n', 'savings_plan_savings_plan_effective_cost', 
        'savings_plan_savings_plan_rate', 'savings_plan_start_time', 'savings_plan_total_commitment_to_date', 
        'savings_plan_used_commitment'
    ]

    bucket(S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

    client = create_boto3_client(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
    try:
        create_export(client, EXPORT_NAME, EXPORT_DESCRIPTION, S3_BUCKET, S3_PREFIX, S3_REGION, query_columns, start_date, end_date )
    except:
        update_export(client, EXPORT_NAME, EXPORT_DESCRIPTION, S3_BUCKET, S3_PREFIX, S3_REGION, query_columns, start_date, end_date )

if __name__ == "__main__":
    bucket_name = os.getenv('BUCKET_NAME')
    bucket_prefix = os.getenv('BUCKET_PREFIX')
    export_name = os.getenv('EXPORT_NAME')
    export_desc = os.getenv('EXPORT_DESCRIPTION')

    main(bucket_name, bucket_prefix, export_name, export_desc)
    
     


