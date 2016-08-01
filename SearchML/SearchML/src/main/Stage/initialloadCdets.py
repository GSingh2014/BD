'''
Created on Jun 3, 2016

@author: gopasing
'''

import requests
import io

cdetsurl = "http://localhost/cgi-bin/ws/ws_ddts_query_new.cgi?expert=Project:CSC.embu,CSC.embu.dev%20and%20(%20Product:wcm-ngwc,katana,wlc,ap,click-ap,mobexp,wcs,location_server,mse,mse-mir,mse-wips,navigator,si-cse,si-sage%20or%20(%20Product:ncs%20and%20org:aoswal,narsunka,rbraj%20)%20)%20%20minus%20Component:documentation,docs,wlc-docs,auto-ap-doc,unified-ap-doc,all-ap-doc,ap-docs,wcm-docs,tme_docs%20and%20Status:S,N,O,W,A,I,R,V,D,F,H,M,P&fields=Identifier,DE-manager,Product,Project,Status,Headline,Found,Component,Urgency,Severity,Customer-name,Software,Hardware,Integrated-releases,Branch,Submitted-on,Attribute,Age,S1S2-without-workaround,Engineer,Keyword,Version,Priority"

def main():
    qddts_cdets = requests.get(cdetsurl)
    qddts_cdetsJson= qddts_cdets.text
    with io.FileIO("/scratch/tux/cdetsweb_prd_Json.json","w") as file:
        file.write(qddts_cdetsJson.encode('utf-8'))
    

if __name__=="__main__":
    print("start")
    main()

