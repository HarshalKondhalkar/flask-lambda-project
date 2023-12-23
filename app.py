from flask import Flask, jsonify, request
import json
from urllib.request import urlopen
import awsgi

app = Flask(__name__)

def get_avg_eod(json_data):
    monthwise_eod = json_data.get('Data', {}).get('Eod analysis', {}).get('EOD MONTH WISE', [])
    eod_list = []
    for eod in monthwise_eod:
        avg_eod = eod.get('averageEod')
        eod_list.append(avg_eod)

    if eod_list is not None:
        eod_list = [eval(value) for value in eod_list]
        final_eod = sum(eod_list)/len(eod_list)
        return final_eod
    else:
        return {"error": "final_eod not extracted"}
    
def get_netinflows(json_data):
    extracted_data = {}
    monthwiseSummary = json_data.get('Data', {}).get('Summary', {}).get('monthwiseSummary', [])
    for summary in monthwiseSummary:
        date = summary.get('monthYear', None)
        netinflows = summary.get('netInflows', None)
        extracted_data[date] = netinflows

    if extracted_data is not None:
        return extracted_data
    else:
        return {"error": "Data not extracted"}
    

def get_transactions(json_data):
    transaction = json_data.get('Data', {}).get('ECS,NACH,CASH Return', {}).get('ECS/NACH RETURN TRANSACTIONS', [])
    transaction_count = 0

    for count in transaction:
        transaction_count = transaction_count + 1

    return transaction_count

def get_netinflows_netoutflows(json_data):
    extracted_data = {}
    monthwiseSummary = json_data.get('Data', {}).get('Summary', {}).get('monthwiseSummary', [])
    for summary in monthwiseSummary:
        date = summary.get('monthYear', None)
        netinflows = summary.get('netInflows', None)
        netoutflows = summary.get('netOutflows', None)
        extracted_data[date] = [netinflows, netoutflows]

    if extracted_data is not None:
        return extracted_data
    else:
        return {"error": "Data not extracted"}
    
def get_avg_netinflows(json_data):
    netinflows = get_netinflows(json_data)
    netinflows_list = []
    for value in netinflows.values():
        netinflows_list.append(value)

    avg_netinflow = sum(netinflows_list)/len(netinflows_list)
    if avg_netinflow is not None:
        return avg_netinflow
    else:
        return {"error": "avg not found"}

def extract_features(json_data):
    netinflows = get_netinflows(json_data)
    netinflows_netoutflows = get_netinflows_netoutflows(json_data)
    transaction_count = get_transactions(json_data)
    avg_eod = get_avg_eod(json_data)
    avg_netinflows = get_avg_netinflows(json_data)

    features_to_extract = {
        'netInflows': netinflows, '(netInflows) - (netOutflows)':netinflows_netoutflows, 
        'ECS/NACH RETURN TRANSACTIONS count':transaction_count, 'averageEOD':avg_eod, 
        'Average netInflows': avg_netinflows
    }
    
    return features_to_extract

    

@app.route('/api/extract_features', methods=['POST'])
def api_extract_features():
    try:
        data = request.json
        s3_url = data.get('s3_url')

        if not s3_url:
            return jsonify({'error': 'Missing s3_url parameter'}), 400

        with urlopen(s3_url) as response:
            json_data = json.load(response)

        result = extract_features(json_data)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def handler(event, context):
    return awsgi.response(app, event, context)
