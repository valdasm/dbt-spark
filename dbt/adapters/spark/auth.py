from adal import AuthenticationContext
from selenium import webdriver
from urllib.parse import urlparse, parse_qs
import time

authority_host_url = "https://login.microsoftonline.com/"
azure_databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"

# Required user input
user_parameters = {

    "tenant" : "2cc4f6be-d233-4968-a92c-74ce2d26adb1",
    "client_id" : "454cf679-c8c3-4495-99e3-cec320145809",
#    "username" : "databricks@valdasmaksimavicius.onmicrosoft.com",
#    "password" : "1KWH8f7YaFpn", #"knit2JOCT1voum*gnul"
   "redirect_uri" : "http://localhost"
}

template_authz_url = ('https://login.windows.net/{}/oauth2/authorize?'+
     'response_type=code&client_id={}&redirect_uri={}&'+
     'state={}&resource={}')
# the auth_state can be a random number or can encoded some info
# about the user. It is used for preventing cross-site request
# forgery attacks
auth_state = 12345
# build the URL to request the authorization code
authorization_url = template_authz_url.format(
            user_parameters['tenant'],
            user_parameters['client_id'],
            user_parameters['redirect_uri'],
            auth_state,
            azure_databricks_resource_id)

def get_authorization_code():
  # open a browser, here assume we use Chrome
  dr = webdriver.Chrome()
  # load the user login page
  dr.get(authorization_url)
  # wait until the user login or kill the process
  code_received = False
  code = ''
  while(not code_received):
      cur_url = dr.current_url
      if cur_url.startswith(user_parameters['redirect_uri']):
          parsed = urlparse(cur_url)
          query = parse_qs(parsed.query)
          code = query['code'][0]
          state = query['state'][0]
          # throw exception if the state does not match
          if state != str(auth_state):
              raise ValueError('state does not match')
          code_received = True
          dr.close()

  if not code_received:
      print('Error in requesting authorization code')
      dr.close()
  # authorization code is returned. If not successful,
  # then an empty code is returned
  return code

def get_refresh_and_access_token():
  # configure AuthenticationContext
  # authority URL and tenant ID are used
  authority_url = authority_host_url + user_parameters['tenant']
  context = AuthenticationContext(authority_url)

  # Obtain the authorization code in by a HTTP request in the browser
  # then copy it here or, call the function above to get the authorization code
  authz_code = get_authorization_code()

  # API call to get the token, the response is a
  # key-value dict
  token_response = context.acquire_token_with_authorization_code(
    authz_code,
    user_parameters['redirect_uri'],
    azure_databricks_resource_id,
    user_parameters['clientId'])

  # you can print all the fields in the token_response
  for key in token_response.keys():
    print(str(key) + ': ' + str(token_response[key]))

  # the tokens can be returned as a pair (or you can return the full
  # token_response)
  return (token_response['accessToken'], token_response['refreshToken'])

print(get_refresh_and_access_token())