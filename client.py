import time
from typing import Optional, Dict, Any, List
from requests import Request, Session, Response
from typing import Dict
import hmac


class AlamedaOtcPortalClient:

    def __init__(self, apikey: str = '', secret: str = '') -> None:
        self._session = Session()
        self._api_key = apikey
        self._api_secret = secret

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('GET', path, params=params)

    def _post(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('POST', path, json=params)

    def _delete(self, path: str) -> Any:
        return self._request('DELETE', path)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        request = Request(method, 'https://api.alamedaotc.com/' + path, **kwargs)
        self._sign_request(request)
        response = self._session.send(request.prepare())
        return self._process_response(response)

    def _sign_request(self, request: Request) -> None:
        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(self._api_secret.encode(), signature_payload, 'sha256').hexdigest()
        request.headers['ALAMEDA-APIKEY'] = self._api_key
        request.headers['ALAMEDA-TIMESTAMP'] = str(ts)
        request.headers['ALAMEDA-SIGNATURE'] = signature

    def _process_response(self, response: Response) -> Any:
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        else:
            if not data['success']:
                raise Exception(data['error'])
            return data['result']

    def get_accepted_quotes(self, limit: int = 10000,
                                before: Optional[300] = None) -> List[Dict]:
        return self._get('otc/quotes/accepted', params={'limit': max(limit, 300),
                                                        **({'before': before} if before else {})})


# Example usage
def fetch_all_accepted_quotes():
    all_accepted_quotes = []
    limit = 300
    before = None
    client = AlamedaOtcPortalClient()  # TODO: enter keys
    while True:
        result = client.get_accepted_quotes(limit=limit, before=before)
        before = min([payment['id'] for payment in result], default=None)
        print(f'Fetched batch of size {len(result)} with min id {before}')
        all_accepted_quotes.extend(result)
        if len(result) < limit:
            break
    return all_accepted_quotes
