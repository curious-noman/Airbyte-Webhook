import json
import os
import functions_framework
from flask import Flask, request, jsonify

app = Flask(__name__)
PORT = int(os.environ.get('PORT', 8080))


@app.route('/', methods=['POST'])
def airbyte_webhook_receiver():
    """
    Cloud Function to receive and process Airbyte webhooks
    """
    try:
        # Get webhook payload
        request_json = request.get_json(silent=True)

        if not request_json or 'data' not in request_json:
            raise ValueError('Invalid Airbyte webhook payload: missing data')

        data = request_json['data']

        # Basic validation to ensure it's an Airbyte webhook
        if not all(k in data for k in ['connection', 'jobId']):
            raise ValueError('Invalid Airbyte webhook payload: missing required fields')

        # Log key information about the sync
        print(f"Received Airbyte sync result for connection: {data['connection']['name']} ({data['connection']['id']})")
        print(f"Job ID: {data['jobId']}")
        print(f"Success: {data['success']}")
        print(f"Records emitted/committed: {data['recordsEmitted']}/{data['recordsCommitted']}")

        # Send a success response back to Airbyte
        return jsonify({
            'success': True,
            'message': 'Webhook received and processed',
            'jobId': data['jobId']
        }), 200

    except Exception as e:
        print(f'Error processing Airbyte webhook: {str(e)}')
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Add a health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    print(f"Starting webhook receiver on port {PORT}")
    # Critical: Make sure to bind to 0.0.0.0, not 127.0.0.1 or localhost
    app.run(host='0.0.0.0', port=PORT, debug=False)