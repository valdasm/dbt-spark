# VM Custom implementation

# https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/app-aad-token#get-token

# MSAL - the latest microsoft auth library for Python
# https://github.com/AzureAD/microsoft-authentication-library-for-python

# from msal import PublicClientApplication

# app = PublicClientApplication(
#     "454cf679-c8c3-4495-99e3-cec320145809",
#     authority="https://login.microsoftonline.com/2cc4f6be-d233-4968-a92c-74ce2d26adb1")

# secret = "a3EBAzongiDoNshk2Am_39~~n-r_3Wwh_I"

# https://login.microsoftonline.com/2cc4f6be-d233-4968-a92c-74ce2d26adb1/oauth2/authorize?client_id=454cf679-c8c3-4495-99e3-cec320145809&response_type=code&redirect_uri=http://localhost&response_mode=query&resource=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d&state=1234

# Returned code
# msal
from adal import AuthenticationContext
# Obtain the authorization code in by a HTTP request in the browser
# then copy it here or, call the function above to get the authorization code
  
authorization_code = "0.ATEAvvbELDPSaEmpLHTOLSatsXn2TEXDyJVEmePOwyAUWAkxAJo.AQABAAIAAAD--DLA3VO7QrddgJg7WevrjrFUXNsYu7Xv1PrByjJUFNw6d3KipKYmTC6x0RRtoG2iRVpx5aI9nRGnxjos_sDZxAMd7eunAPFiN-SHATS16ycqDvKf-Kl9-LEW1lrSmSBO6efEyesFpyBC-SwfDQ7KOyjCvTchFC67ZrjF9zVlV0xZ8RF0WYEW7BoOD01WJ1pXe9-3Od5ACBQGRThF_oglvcF6gyjI892tqiepYKnHZCisXDv-5XLQLPUf8ZNcUG3I83zXXtr7suIBVozlu4DKoI16R-R0joCTJwycv_LW6dDQaHedkuiGhQ6G9LEnQuTOURsvj68jr_RO-C3lEkvG8plKxVqk-xlAXDKkU4tV1OkyUBiM3gcELJjSWlku180R0O4DdDSGT9Y7oZePl3zhogoMq_rQTj8sqNJFOQZ5SIkoPkv1dvoBeg6-Sl-VUxAdNPnrO5IH0mgSOqC-uaMXIULYgH-i5C4EGFiz9iPVRMa2V5aIXRHm-xVNXVpg3tFmtbc4hbFDOP52IZqGcR3vvrsz_3EUqJzWE0Q6S4tknnl9RYwJXQc6fP70Mefl4EuIha2BYSaaJTG67yL8ufi1zD_8i9X_jZvIrS7-4ZNElHEXEFSLs5_lMEaR6vVOnOJWZMy5yCEUxUYjZRI8EcVjXDOB2B9nrW7FzGIpy0lQc3-W5buz0qBCIP8tHNIAtNK6GRn5VDXKfKC_oKVhazozFc0hSrVrtRWiRO_43_KAMm1fGnN_ZAZ1CLZpf45bd1LKk4B_Wc3iq-K8fdFua-Xri8CP2FTT5dv3g8mdDjrgRBq-GeSj5r-poHnmc0IZu_U_dMC3fQX7w3JMoVEhp8v0gsWW-Sp_KHmwHyXyRvevK3p4p1SB4yeFJKmV4FD98HA5KEnvvSiWQWAXuvYI2avxem_pKMMojVdsmqvMiXnpOBJ7ZCIRrX4R3LcYW2zhHhJQzlI3gJtxJKkeDyyAzbRWUS3o2QQyYZGTXcdg4j3G0q-JE9YSx8klqPrOu9P9xveuO7tnuM645swjRTAN8vvVNyAGeKyBIm4oeyzOirtaX4MtdKm_CCnyaMJFCr84RPdbjm5dF0eT3NGFpMPKPzmBJfpxh4FOfchZZzXJq0zYeEsVsNBqvGmpsy75hGuEjmYJmxedo8lV9BlC4w_r26Oa4AhQdSSHFlXFvSoYSqdLBiILpZBHrPf_n5hMTVksaLlLT__0rjvuMzscKbjpSU_Srdui7Nm_kWA4vCrmKv1pCCAA"
authority_host_url = "https://login.microsoftonline.com/"
azure_databricks_resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"


# # Required user input
user_parameters = {
   "tenant" : "2cc4f6be-d233-4968-a92c-74ce2d26adb1",
   "client_id" : "454cf679-c8c3-4495-99e3-cec320145809",
   "client_secret": "Gs~img8N.r0.eR.~5h1u~Tx.Mz-fP_Pr37",
   "username" : "databricks@valdasmaksimavicius.onmicrosoft.com",
   "password" : "1KWH8f7YaFpn", #"knit2JOCT1voum*gnul"
   "redirect_uri" : "http://localhost"

}

authority_url = authority_host_url + user_parameters['tenant']

def get_refresh_and_access_token():
  # configure AuthenticationContext
  # authority URL and tenant ID are used
  
  context = AuthenticationContext(authority_url)

  

  # API call to get the token, the response is a
  # key-value dict
  token_response = context.acquire_token_with_authorization_code(
    authorization_code,
    user_parameters['redirect_uri'],
    azure_databricks_resource_id,
    user_parameters['client_id'])

  # you can print all the fields in the token_response
  for key in token_response.keys():
    print(str(key) + ': ' + str(token_response[key]))

  # the tokens can be returned as a pair (or you can return the full
  # token_response)
  return (token_response['accessToken'], token_response['refreshToken'])



# supply the refresh_token (whose default lifetime is 90 days or longer [token lifetime])
def refresh_access_token(refresh_token):
  context = AuthenticationContext(authority_url)
  # function link
  token_response = context.acquire_token_with_refresh_token(
                  refresh_token,
                  user_parameters['client_id'],
                  azure_databricks_resource_id)
  # print all the fields in the token_response
  for key in token_response.keys():
      print(str(key) + ': ' + str(token_response[key]))

  # the new 'refreshToken' and  'accessToken' will be returned
  return (token_response['refreshToken'], token_response['accessToken'])

print(get_refresh_and_access_token())
# refreshToken = """0.ATEAvvbELDPSaEmpLHTOLSatsXn2TEXDyJVEmePOwyAUWAkxAJo.AgABAAAAAAD--DLA3VO7QrddgJg7WevrAgDs_wQA9P8VHkHDubbSToigD6ot5bRISi9hRbx-1R1xFSAf358UI7LpdKQUcnpW5tKr3gKjpOobqVxvae-yA9w0ECZGelKsMy-Gj2961dhIHx_7Aae_IUnkr-tyiQDHzHInl-UT5kM1zgt_20qAUDLOPmChhJDsSAql5wD__3LtRT5p4rJSSrttH1zbnn7cew6dTOFtTQrAaqURSn5a2S9LAyLcTVHiXWSoW942fthUjN_vr4cXh_JrpCdNkf_PR39q86HQQog_Km08oXK21pSrhfSvzbTHvenFM5yB15TAn_o_KMyGGHPnYMrM4XicRTGAV0r_U43loF9uSzB1JHMem0rsncdvLnbjcGdS2-Zu7-kjH78rFrLGsoGPYCwvV5w0VgcOHgP3x-uC3I-rLJsbJk2ZRNRcfgqU3DbMZrbXgfzVaUjxJfwpVml-DBmC5cdj9QdKYqIH-OTmkvaIfePQgOhhNdJbEBmU38lYBe9T_2PD_meDGSYdtDdZwGvdRZvlnOkuIScWJRy_54dgjp-l5o4GM3geh35BFStG0Jk5yijfFByNxTNR4hai_Ki_3a3wycTGDdK3vXDA6YQwjGC_gvqBW_P5-fmQESlCvlcF3sbP5XdWjWafij8HxNmsq_I7mxjNH4RDzPHz_g6W19NNUFXBiEK5twrSgL-GIT6ymyC8eO3R45JC7CEdguh1UWJnt87qX04er79ffdt1ofA4CahiOo4YORZrlFoCk4zhln2aCYUQEyLU7bYKT2grd-ED4sLUVdCkHsp8CN-25uKdp8LnMEEMejTEc7Umy4kljEZ7aCrEoqTUU1us8ppMU1T8VC_g_CbP23C7V-to0ytellVt1vkXWEIn5Y5E3M-paOlnGFRn3wGkFAvnaXygTGAOlrCSGD_FOjkxJmpBQ5J2BUxBeQUCsV_B_SomaHkA5GDHtP65O7TMitHXzOMt9WanuiKs0zE2exlGfCA9eIMAkxhDWzM4uaaQE0pUXh6RHqREGwwTcjQrSDfrKHU6O1H3UPOBAaj5zkaeGws_x2pz4vPCRNiYP3CFCwtzCKWOXzsEJQHpZOYsN86uNKp_GLL1hsBG8MeQiXMuKvotQESpZQi_kRDsyFA1GWAu3FDfkrhBB8_nN3q6qgxZvyXfYnmpaGbkeZoXThH_KPV8HWhSQZSyq6u0okWb0cQDKI2FeYKKwRUa2cCpUnJldwdnDLlrsUHbKRIaXDbngx39AmQTRMJRyb5mPiQOrytYvPU3z3WOReZhpnTfYDhgCn_1GqAVZoVw8ZTaMR6Aw7xrWfTJbD4kxBGEQ54HE9hDj75uOEyvy9EhBXTGrBw36NyWLhI3upDPpOnuMm7hL9aY9s10P47qTEHZEv1MfnWRVXpxlcyX1jlHEsmMEfzW8KmuypCatXQgvHyYYIgGsvTZUicSKlirfRYLJVswqS3ApTcks1MlmQLivOh4ly6_xNf-M3jdrLmFVMuiS3t58h3qIrJxEcuFIv0HJWmh3EbUTirs1aSOYy7S5qUvV2hhj5tuul0x9DhiFTCk93Ogcjd56W4P9yEHVDIWBmSkv0mr7kEDpcNYW_uUhIKVcWJrNcc292sjn8ZE0_ApOGvMJ7I0pn-BKATDvZcaTiTKexcvhd44UXyyRm2fu5n7ikPqlJYsDbaQ33CX6S4kbipRBE6OdH-ISAaZHgUY0srlmpLTn5yFe_kLjefijwCBD519pe5FZZW5xRGWtstkfqpXOtFMrzPfOmvBiFOpirYt0UCIw06cstJcXlNaRLVIaQpQUN14-cPVYgK_4zPSC6kc957w_9O-WGxb-pKQdK4YaJGqhNX8ySdCsBSG3g
# ('0.ATEAvvbELDPSaEmpLHTOLSatsXn2TEXDyJVEmePOwyAUWAkxAJo.AgABAAAAAAD--DLA3VO7QrddgJg7WevrAgDs_wQA9P8VHkHDubbSToigD6ot5bRISi9hRbx-1R1xFSAf358UI7LpdKQUcnpW5tKr3gKjpOobqVxvae-yA9w0ECZGelKsMy-Gj2961dhIHx_7Aae_IUnkr-tyiQDHzHInl-UT5kM1zgt_20qAUDLOPmChhJDsSAql5wD__3LtRT5p4rJSSrttH1zbnn7cew6dTOFtTQrAaqURSn5a2S9LAyLcTVHiXWSoW942fthUjN_vr4cXh_JrpCdNkf_PR39q86HQQog_Km08oXK21pSrhfSvzbTHvenFM5yB15TAn_o_KMyGGHPnYMrM4XicRTGAV0r_U43loF9uSzB1JHMem0rsncdvLnbjcGdS2-Zu7-kjH78rFrLGsoGPYCwvV5w0VgcOHgP3x-uC3I-rLJsbJk2ZRNRcfgqU3DbMZrbXgfzVaUjxJfwpVml-DBmC5cdj9QdKYqIH-OTmkvaIfePQgOhhNdJbEBmU38lYBe9T_2PD_meDGSYdtDdZwGvdRZvlnOkuIScWJRy_54dgjp-l5o4GM3geh35BFStG0Jk5yijfFByNxTNR4hai_Ki_3a3wycTGDdK3vXDA6YQwjGC_gvqBW_P5-fmQESlCvlcF3sbP5XdWjWafij8HxNmsq_I7mxjNH4RDzPHz_g6W19NNUFXBiEK5twrSgL-GIT6ymyC8eO3R45JC7CEdguh1UWJnt87qX04er79ffdt1ofA4CahiOo4YORZrlFoCk4zhln2aCYUQEyLU7bYKT2grd-ED4sLUVdCkHsp8CN-25uKdp8LnMEEMejTEc7Umy4kljEZ7aCrEoqTUU1us8ppMU1T8VC_g_CbP23C7V-to0ytellVt1vkXWEIn5Y5E3M-paOlnGFRn3wGkFAvnaXygTGAOlrCSGD_FOjkxJmpBQ5J2BUxBeQUCsV_B_SomaHkA5GDHtP65O7TMitHXzOMt9WanuiKs0zE2exlGfCA9eIMAkxhDWzM4uaaQE0pUXh6RHqREGwwTcjQrSDfrKHU6O1H3UPOBAaj5zkaeGws_x2pz4vPCRNiYP3CFCwtzCKWOXzsEJQHpZOYsN86uNKp_GLL1hsBG8MeQiXMuKvotQESpZQi_kRDsyFA1GWAu3FDfkrhBB8_nN3q6qgxZvyXfYnmpaGbkeZoXThH_KPV8HWhSQZSyq6u0okWb0cQDKI2FeYKKwRUa2cCpUnJldwdnDLlrsUHbKRIaXDbngx39AmQTRMJRyb5mPiQOrytYvPU3z3WOReZhpnTfYDhgCn_1GqAVZoVw8ZTaMR6Aw7xrWfTJbD4kxBGEQ54HE9hDj75uOEyvy9EhBXTGrBw36NyWLhI3upDPpOnuMm7hL9aY9s10P47qTEHZEv1MfnWRVXpxlcyX1jlHEsmMEfzW8KmuypCatXQgvHyYYIgGsvTZUicSKlirfRYLJVswqS3ApTcks1MlmQLivOh4ly6_xNf-M3jdrLmFVMuiS3t58h3qIrJxEcuFIv0HJWmh3EbUTirs1aSOYy7S5qUvV2hhj5tuul0x9DhiFTCk93Ogcjd56W4P9yEHVDIWBmSkv0mr7kEDpcNYW_uUhIKVcWJrNcc292sjn8ZE0_ApOGvMJ7I0pn-BKATDvZcaTiTKexcvhd44UXyyRm2fu5n7ikPqlJYsDbaQ33CX6S4kbipRBE6OdH-ISAaZHgUY0srlmpLTn5yFe_kLjefijwCBD519pe5FZZW5xRGWtstkfqpXOtFMrzPfOmvBiFOpirYt0UCIw06cstJcXlNaRLVIaQpQUN14-cPVYgK_4zPSC6kc957w_9O-WGxb-pKQdK4YaJGqhNX8ySdCsBSG3g', 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyIsImtpZCI6Im5PbzNaRHJPRFhFSzFqS1doWHNsSFJfS1hFZyJ9.eyJhdWQiOiIyZmY4MTRhNi0zMzA0LTRhYjgtODVjYi1jZDBlNmY4NzljMWQiLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC8yY2M0ZjZiZS1kMjMzLTQ5NjgtYTkyYy03NGNlMmQyNmFkYjEvIiwiaWF0IjoxNjEzODQ0OTI1LCJuYmYiOjE2MTM4NDQ5MjUsImV4cCI6MTYxMzg0ODgyNSwiYWNyIjoiMSIsImFpbyI6IkFVUUF1LzhUQUFBQXo0Zm1TU2hXaHRYUzRhcXpxUlFGM1RmdDhGYTYrOXo4U2c4a1FVWXVCQXpsUEo0M0RHOHZsb3lxbk1FMDYzRC94SzRSRG9QMXh3MUtGYXpYRVlXdkN3PT0iLCJhbHRzZWNpZCI6IjE6bGl2ZS5jb206MDAwM0JGRkQxM0FBQkU3RiIsImFtciI6WyJwd2QiXSwiYXBwaWQiOiI0NTRjZjY3OS1jOGMzLTQ0OTUtOTllMy1jZWMzMjAxNDU4MDkiLCJhcHBpZGFjciI6IjAiLCJlbWFpbCI6InZhbGRhc0BtYWtzaW1hdmljaXVzLmV1IiwiZmFtaWx5X25hbWUiOiIyZGRhMWYzYS0zMjExLTRjMjAtYTdjMC1jYzliMmY3ZmI4NzkiLCJnaXZlbl9uYW1lIjoiZjk1ZWQ5NzgtY2I0My00OThjLWIxNmUtMTU0MGQ5NWYzNDQxIiwiaWRwIjoibGl2ZS5jb20iLCJpcGFkZHIiOiI3OC41OC4yMzcuMTY2IiwibmFtZSI6ImY5NWVkOTc4LWNiNDMtNDk4Yy1iMTZlLTE1NDBkOTVmMzQ0MSAyZGRhMWYzYS0zMjExLTRjMjAtYTdjMC1jYzliMmY3ZmI4NzkiLCJvaWQiOiIwYjRmNWQ3OC03ZTBlLTQyMzItYmI2MS01NjNmZmVlNDlhNzQiLCJwdWlkIjoiMTAwMzAwMDBBQzAwN0U3NyIsInJoIjoiMC5BVEVBdnZiRUxEUFNhRW1wTEhUT0xTYXRzWG4yVEVYRHlKVkVtZVBPd3lBVVdBa3hBSm8uIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiVGNpaS16bXhDUTkxSjdCWVoxdGpKdkVSb0xLQ0FGQ2NPeGYtYmo3QzF3SSIsInRpZCI6IjJjYzRmNmJlLWQyMzMtNDk2OC1hOTJjLTc0Y2UyZDI2YWRiMSIsInVuaXF1ZV9uYW1lIjoibGl2ZS5jb20jdmFsZGFzQG1ha3NpbWF2aWNpdXMuZXUiLCJ1dGkiOiItWXQtSGUwM1NVcTFaYmlubC00TkFBIiwidmVyIjoiMS4wIn0.IfKrnyIsapsGlrflhm5hdnTho0IksI-fKjmB0fMtQybABuLuIncSzNoDF43-zWiumi-tK2jrTf0iM8RbGklhB9ldnJ5mSiJJWBJf9dH-Sva2yLp3XblExFvrdnCmp4ExZH0u3shZNnactBikoSzTp3JP2F-vwojepB1bWOYNwy5m-gabOgOu5bse-xrfrgTVXINkPaeXCOrri0d-41Q9DqG1fyUMNsKSQkjGvq55vPvPkdmhaYAtYxF7MjPuyYk8pcQHpc0h77nGtjPoZnTV9lwYV19C9FFiYrSEhXTfGh6SREQQ7QHC70iK4-OF_mNW8aLNGUiwaZ4gqPdPcvPhvw"""
# print(refresh_access_token(refreshToken))

# from msal import ConfidentialClientApplication

# app = ConfidentialClientApplication(
#     client_id=user_parameters['client_id'], 
#     client_credential=user_parameters['client_secret'],
#     authority="https://login.microsoftonline.com/" + user_parameters['tenant'])

# result = None

# config = {
#     "scope": ["https://graph.microsoft.com/.default"],
# }

# if not result:
 
#     result = app.acquire_token_for_client(scopes=config["scope"])

# if "access_token" in result:
#     # Call a protected API with the access token.
#     print(result["token_type"])
#     print(result)
# else:
#     print(result.get("error"))
#     print(result.get("error_description"))
#     print(result.get("correlation_id"))  # You might need this when reporting a bug.

