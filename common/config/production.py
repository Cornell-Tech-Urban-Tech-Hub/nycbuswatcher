config = {
    'base_api_url' : 'https://api.buswatcher.org:80/api/v2/nyc',
    'shipment_api_url' : 'https://api.buswatcher.org:80/api/v2/nyc/{}/{}/{}/{}/{}/buses',# hostname and port inside the stack
    'glacier_api_url' : 'https://api.buswatcher.org:80/api/v2/nyc/{}/{}/{}/{}/{}/archive',
    'history_api_url' : 'https://api.buswatcher.org:80/api/v2/nyc/{}/history',
    'http_connections': 20
}