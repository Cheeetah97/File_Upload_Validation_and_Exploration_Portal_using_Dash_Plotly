import base64
import os
from dash import Dash, dcc, html, Input, Output, callback, State, ctx, MATCH, dash_table, no_update
import dash_bootstrap_components as dbc
import dash_loading_spinners as dls
from dash.exceptions import PreventUpdate
import io
import pandas as pd
import feffery_antd_components as fac
from datetime import datetime
import copy
from PIO_VALIDATION import pio_temp_upload, pio_duplicate_check, pio_missing_check, pio_nosales_check, pio_po_before_pi_check
from AES_VALIDATION import aes_temp_upload,aes_missing_skus_check
from All_Upload import pio_upload,aes_upload,sales_upload,stocks_upload,production_upload,dawpred_upload
import subprocess
import sys

app = Dash(__name__,suppress_callback_exceptions=True)
app.config.external_stylesheets = [dbc.themes.LUX]
    
#%%
# App Layout
accordion = html.Div(dbc.Accordion([],id="mapping-file-list",flush=True,start_collapsed=True))
accordion_2 = html.Div(dbc.Accordion([],id="validation-file-list",flush=True,start_collapsed=True))

navbar = dbc.Navbar(dbc.Container([html.Div(dbc.Row([dbc.Col(html.Img(src=app.get_asset_url('AI_LOGO.png'),className="nav-img",id="LOGO")),
                                                    ],align="left",className="nav-row"))],fluid=True,className="nav-container"),light=True,className="navbar")

icons = ['antd-cloud-upload','antd-file-protect']
name_icon =['Files Upload','Data Validation']

new_style_loading={"width": "100%",
                   "borderWidth": "3px",
                   "lineHeight": "500%",
                   "borderStyle": "dashed",
                   "borderRadius": "30px",
                   "borderColor": "#00897b",
                   "textAlign": "center",
                   "margin": "auto",
                   "fontSize": "20px",
                  }

orig_style_loading={"width": "100%",
                   "borderWidth": "3px",
                   "lineHeight": "1500%",
                   "borderStyle": "dashed",
                   "borderRadius": "30px",
                   "borderColor": "#00897b",
                   "textAlign": "center",
                   "margin": "auto",
                   "fontSize": "20px",
                  }

app.layout = html.Div([dbc.Row([
                                  navbar,
                                  dbc.Col( [fac.AntdMenu(id='menu-demo',
                                                         menuItems=[{'component':'button','props':{'key':name_icon[j],'title':name_icon[j],'icon':icon}}for j,icon in enumerate(icons)],
                                                         mode='inline',theme='dark',renderCollapsedButton=True,defaultSelectedKey=None,style={"background-color":"#005480","min-height":"81.3vh"})],
                                           width={"size":"auto"},style={"height":"fit-content"}),
                
                                  dbc.Col( [dbc.Row([html.Div([ dls.Hash(id="loading",children=html.Div(id="loading-output",hidden=True),fullscreen=True,color='#00897',speed_multiplier=2,size=100,fullscreenClassName='fullscreen-load',show_initially=False),
                                                                dls.Hash(id="final_upload",children=html.Div(id="final_upload_output",hidden=True),fullscreen=True,color='#00897',speed_multiplier=2,size=100,fullscreenClassName='fullscreen-load',show_initially=False),
                                                                html.Div([fac.AntdIcon(icon="fc-approval",style={"fontSize":"100px","marginLeft":"45%","marginTop":"0px"}),
                                                                          html.Div([html.P("Thank You!",id="exit-header",hidden=False)],id="exit-div"),
                                                                          html.P("All the data is uploaded to the knowledgebase",id="exit-para",hidden=False)],id="exit-block",hidden=True),
                                                                html.Div([html.Div([html.H1("Welcome",id="welcome-header",hidden=False),
                                                                                    html.P("to the Data Upload Portal",id="welcome-header-l2",hidden=False),
                                                                                    html.P("This data portal is designed to clean and validate all the input data formats before uploading it to the knowledge base",id="welcome-para",hidden=False),
                                                                                    dbc.Button("Continue",id='continue-button',n_clicks=0,class_name="btn btn-outline-secondary",color='secondary')],id="welcome-div"),
                                                                          html.Img(src=app.get_asset_url('Logo.png'),className="welcome-img")],id="welcome-block",hidden=False),
                                                                html.H2("Upload Relevant Files",id="upload-header",hidden=True),
                                                                html.Div(dcc.Upload(children = ['Drag and Drop or ',html.A('Select Files',href="#",style={"color":"blue"})],
                                                                         id = "mapping-upload-data",
                                                                         style = orig_style_loading,
                                                                         disabled = False,
                                                                         multiple = True),id="upload_div",hidden=True),
                                                                html.Div([html.Div("No Files Uploaded. Please check filenames!",id="no-files-message",className="alert alert-danger",hidden=True),
                                                                          html.Div("One or more files not uploaded. Please check filenames!",id="less-files-message",className="alert alert-warning",hidden=True),
                                                                          html.H3("",id = "mapping-file-header"),
                                                                          accordion,
                                                                          accordion_2,
                                                                          html.Div("No Issues Found!",id="no-issues-message",className="alert alert-success",hidden=True),
                                                                          dbc.Button("Proceed",className="proceed-button span",id='proceed-button',n_clicks=0,style={"float":"right","display":"none"}),
                                                                          dcc.ConfirmDialog(id='confirm-danger',message='All the files will be uploaded to Database. Are you sure you want to continue?')],id="file-container",hidden=True),
                                                               ],id="wrapper-div")],id="wrapper-row")],id="content-col")
                                ],style={"height":"fit-content"})
                       ],style={"height":"fit-content"})

#%%
# Functions
def parse_contents(contents,filename):
    content_type,content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        if '.csv' in filename:
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
        elif '.xlsx' in filename:
            df = pd.read_excel(io.BytesIO(decoded))
        else:
            print("NONE")
    except Exception as e:
        print(e)
        return html.Div(['There was an error processing this file.'])
    
    return df

def check_column_names(data,filename):
    if "Predictions" in filename:
        columns_required = ['Material','Category','TYear','TMonth','Prediction']
        columns_required_LC = list(map(lambda x: x.lower(),columns_required))
        columns_present_LC = list(map(lambda x: x.lower(),list(data.columns)))
        columns_absent_lst = [columns_required_LC.index(x) for x in columns_required_LC if x not in columns_present_LC]
        columns_absent = [columns_required[index] for index in columns_absent_lst]
        return columns_absent
    
    elif "Master_Coding" in filename:
        columns_required = ['Master ID','Product','Material','Material Description','Measurement Instrument','Colour Similarity','Product type','Function','Series','Colour','Key Feature']
        columns_required_LC = list(map(lambda x: x.lower(),columns_required))
        columns_present_LC = list(map(lambda x: x.lower(),list(data.columns)))
        columns_absent_lst = [columns_required_LC.index(x) for x in columns_required_LC if x not in columns_present_LC]
        columns_absent = [columns_required[index] for index in columns_absent_lst]
        return columns_absent
    
    elif "Production_Numbers" in filename:
        columns_required = ['TYear','TMonth','Material','Material Description','Quantity']
        columns_required_LC = list(map(lambda x: x.lower(),columns_required))
        columns_present_LC = list(map(lambda x: x.lower(),list(data.columns)))
        columns_absent_lst = [columns_required_LC.index(x) for x in columns_required_LC if x not in columns_present_LC]
        columns_absent = [columns_required[index] for index in columns_absent_lst]
        return columns_absent
    
    elif "Regional_Closing_Stocks" in filename:
        columns_required = ['Plant Name','Material','Material Description','Storage L','Storage LName','Plant Code','Batch','Onhand Qty','UOM','Intransit','Total Qty']
        columns_required_LC = list(map(lambda x: x.lower(),columns_required))
        columns_present_LC = list(map(lambda x: x.lower(),list(data.columns)))
        columns_absent_lst = [columns_required_LC.index(x) for x in columns_required_LC if x not in columns_present_LC]
        columns_absent = [columns_required[index] for index in columns_absent_lst]
        return columns_absent
    
    elif "Closing_Sales" in filename:
        columns_required = ['Sales Office Description','Payer','Payer Name','Item text','Billing Document','Sales Document Type','Sales Document',
                            'Billing Date','Due Date','Material','Material Description',
                            'Sales Order Item Created Date','Descr. of Storage Loc.',
                            'Document Currency','ZBTP value','ZPK0 value','Billing qty in SKU',
                            'MWST value','ZPT2 value','Sold-to Party','Sold-to party code',
                            'Sales Representative','Sales Representative Code','Product']
        columns_required_LC = list(map(lambda x: x.lower(),columns_required))
        columns_present_LC = list(map(lambda x: x.lower(),list(data.columns)))
        columns_absent_lst = [columns_required_LC.index(x) for x in columns_required_LC if x not in columns_present_LC]
        columns_absent = [columns_required[index] for index in columns_absent_lst]
        return columns_absent
    
    elif "Opening_Stocks" in filename:
        columns_required = ['Warehouse_code','Warehouse','Product','Material','Material_name','model','series','TTL']
        columns_required_LC = list(map(lambda x: x.lower(),columns_required))
        columns_present_LC = list(map(lambda x: x.lower(),list(data.columns)))
        columns_absent_lst = [columns_required_LC.index(x) for x in columns_required_LC if x not in columns_present_LC]
        columns_absent = [columns_required[index] for index in columns_absent_lst]
        return columns_absent
    
    elif "Phase_In_Out" in filename:
        columns_required = ['Product','Material','Material Description','Phase Out-(revised)','Phase In Date-Revised','Sales Group','Price Group']
        columns_required_LC = list(map(lambda x: x.lower(),columns_required))
        columns_present_LC = list(map(lambda x: x.lower(),list(data.columns)))
        columns_absent_lst = [columns_required_LC.index(x) for x in columns_required_LC if x not in columns_present_LC]
        columns_absent = [columns_required[index] for index in columns_absent_lst]
        return columns_absent
    else:
        return None

def update_column(col_org,col_new,file_contents,options,loc,filename):
    file_contents = file_contents.rename(columns={col_new:col_org})
    del options[loc]
    filename.replace('.txt','.xlsx')
    filename.replace('.csv','.xlsx')
    file_contents.to_excel(f"{filename}",index=False)
    return options
    
def validation(data,filename,uploaded_filenames):
    if "Phase_In_Out" in filename:
        childs = []
        titles = []
        pio_temp = pio_temp_upload(data)
        
        if pio_temp == True:
            
            pio_duplicates = pio_duplicate_check()
            if len(pio_duplicates) != 0:
                childs.append(pio_duplicates)
                titles.append("Duplicate SKUs")
                
            pio_po_before_pi = pio_po_before_pi_check()
            if len(pio_po_before_pi) != 0:
                childs.append(pio_po_before_pi)
                titles.append("Phase Out before Phase In")
            
            try:
                if any("Closing_Sales" in f for f in uploaded_filenames)|(int(datetime.today().month) == int(datetime.fromtimestamp(os.path.getmtime("Closing_Sales.xlsx")).month)):
                    sales = pd.read_excel("Closing_Sales.xlsx")
                    pio_missing = pio_missing_check(sales)
                    if len(pio_missing) != 0:
                        childs.append(pio_missing)
                        titles.append("Missing SKUs")
                    pio_nosales = pio_nosales_check()
                    if len(pio_nosales) != 0:
                        childs.append(pio_nosales)
                        titles.append("Continued Model but no Sales")  
                    return childs,titles
                else:
                    return childs,titles
            except:
                return childs,titles
        else:
            return None,None
    elif "Master_Coding" in filename:
        childs = []
        titles = []
        aes_temp = aes_temp_upload(data)
        if aes_temp == True:
            aes_missing = aes_missing_skus_check()
            if len(aes_missing) != 0:
                childs.append(aes_missing)
                titles.append("Missing SKUs")
            return childs,titles
        else:
            return None,None
    elif ("Predictions" in filename)&(any("Phase_In_Out" in f for f in uploaded_filenames)):
        return None,None
    else:
        return None,None
        
        
#%% 
# Save uploaded files and regenerate the file list
@callback(Output("loading-output","children"),
          Output("mapping-file-list","children"),
          Output("file-container","hidden"),
          Output("mapping-file-header","children"),
          Output("validation-file-list","children"),
          Output('menu-demo','currentKey'),
          Output("upload_div","hidden"),
          Output("upload-header","hidden"),
          Output("mapping-upload-data","style"),
          Output("proceed-button","style"),
          Output("welcome-block","hidden"),
          Output("exit-block","hidden"),
          Output("continue-button","n_clicks"),
          Output("no-files-message","hidden"),
          Output("less-files-message","hidden"),
          Output("no-issues-message","hidden"),
          Input("continue-button","n_clicks"),
          Input("mapping-upload-data","filename"), 
          Input("mapping-upload-data","contents"),
          Input('menu-demo','currentKey'),
          Input('confirm-danger','submit_n_clicks'),
          prevent_initial_call=True)
def file_upload__or__file_validate(n_clicks,uploaded_filenames,uploaded_file_contents,page_id,submit_n_clicks):
    file_list = []
    if (page_id == name_icon[0])|(n_clicks>0):
        if uploaded_filenames is not None and uploaded_file_contents is not None and ctx.triggered_id == "mapping-upload-data":
            for filename,contents in zip(uploaded_filenames,uploaded_file_contents):
                data = parse_contents(contents,filename)
                missing_columns = check_column_names(data,filename)
                filename.replace('.txt','.xlsx')
                filename.replace('.csv','.xlsx')
                data.to_excel(f"{filename}",index=False)
                
                if missing_columns is not None:
                    if len(missing_columns) == 0:  
                        file_list.append(dbc.AccordionItem([dls.Hash(id={"type":"loading","index":"c-"+str(uploaded_filenames.index(filename))},children=html.Div(id={"type":"loading-output","index":"c-"+str(uploaded_filenames.index(filename))},hidden=True),color='#00897',speed_multiplier=2,size=100,show_initially=False,fullscreenClassName="fullscreen-load",fullscreen=True),
                                                            html.Div([html.Div(id={"type":"c-message","index":"c-"+str(uploaded_filenames.index(filename))},className="alert alert-success",hidden=True),
                                                                      html.Div(id={"type":"ic-columns","index":"c-"+str(uploaded_filenames.index(filename))},className="ic-columns",hidden=True),
                                                                      html.Div(dcc.Dropdown(id={"type":"ic-columns-dropdown","index":"c-"+str(uploaded_filenames.index(filename))},placeholder="Find a Mapping"),id={"type":"div-ic-columns-dropdown","index":"c-"+str(uploaded_filenames.index(filename))},className="div-ic-columns-dropdown",hidden=True),
                                                                      dbc.Button("Update",className="select-button span",id={"type":"select-button","index":"c-"+str(uploaded_filenames.index(filename))},n_clicks=0,style={"display":"none"})],id={"type":"info-block","index":"c-"+str(uploaded_filenames.index(filename))},className="info-block",hidden=True)],title=filename,id={"type":"f-button","index":"c-"+str(uploaded_filenames.index(filename))},class_name="c-acc-item"))
                    else:
                        file_list.append(dbc.AccordionItem([dls.Hash(id={"type":"loading","index":"ic-"+str(uploaded_filenames.index(filename))},children=html.Div(id={"type":"loading-output","index":"ic-"+str(uploaded_filenames.index(filename))},hidden=True),color='#00897',speed_multiplier=2,size=100,show_initially=False,fullscreenClassName="fullscreen-load",fullscreen=True),
                                                            dls.Hash(id={"type":"loading2","index":"ic-"+str(uploaded_filenames.index(filename))},children=html.Div(id={"type":"loading-output2","index":"ic-"+str(uploaded_filenames.index(filename))},hidden=True),color='#00897',speed_multiplier=2,size=100,show_initially=False,fullscreenClassName="fullscreen-load",fullscreen=True),
                                                            html.Div([html.Div(id={"type":"c-message","index":"ic-"+str(uploaded_filenames.index(filename))},className="alert alert-success",hidden=True),
                                                                      html.Div(id={"type":"ic-columns","index":"ic-"+str(uploaded_filenames.index(filename))},className="ic-columns",hidden=True),
                                                                      html.Div(dcc.Dropdown(id={"type":"ic-columns-dropdown","index":"ic-"+str(uploaded_filenames.index(filename))},placeholder="Find a Mapping"),id={"type":"div-ic-columns-dropdown","index":"ic-"+str(uploaded_filenames.index(filename))},className="div-ic-columns-dropdown",hidden=True),
                                                                      dbc.Button("Update",className="select-button span",id={"type":"select-button","index":"ic-"+str(uploaded_filenames.index(filename))},n_clicks=0,style={"display":"none"})],id={"type":"info-block","index":"ic-"+str(uploaded_filenames.index(filename))},className="info-block",hidden=True)],title=filename,id={"type":"f-button","index":"ic-"+str(uploaded_filenames.index(filename))},class_name="ic-acc-item"))
            if len(file_list)==0:
                return "",file_list,False,"",[],name_icon[0],False,False,no_update,no_update,True,True,0,False,True,True
            elif len(file_list)<len(uploaded_filenames):
                return "",file_list,False,"Uploaded Files",[],name_icon[0],False,False,new_style_loading,no_update,True,True,0,True,False,True
            else:
                return "",file_list,False,"Uploaded Files",[],name_icon[0],False,False,new_style_loading,no_update,True,True,0,True,True,True
        
        elif ctx.triggered_id != "mapping-upload-data":
            return "",[],True,"",[],name_icon[0],False,False,orig_style_loading,no_update,True,True,0,True,True,True
        else:
            raise PreventUpdate
            
    elif (page_id == name_icon[1]) & (submit_n_clicks is None):
        if uploaded_filenames is not None and uploaded_file_contents is not None:
            uploaded_filenames.sort(reverse=True) 
            u_filenames = copy.deepcopy(uploaded_filenames)
            u_filecontents = copy.deepcopy(uploaded_file_contents)
            for filename,contents in zip(uploaded_filenames,uploaded_file_contents):
                if ".csv" in filename:
                    data = pd.read_csv(f"{filename}")
                elif '.xlsx' in filename:
                    data = pd.read_excel(f"{filename}")
                
                if check_column_names(data,filename) is not None:
                    
                    if len(check_column_names(data,filename)) > 0:
                        return "",no_update,no_update,no_update,no_update,name_icon[0],False,False,no_update,no_update,True,True,0,True,True,True
                else:
                    continue
                
                issues_df,issues_title = validation(data,filename,uploaded_filenames)
                if issues_df is None:
                    index_r = u_filenames.index(filename)
                    del u_filenames[index_r]
                    del u_filecontents[index_r]
                else:
                    file_list.append(dbc.AccordionItem([dls.Hash(id={"type":"v-loading","index":"v-"+str(u_filenames.index(filename))},children=html.Div(id={"type":"v-loading-output","index":"v-"+str(u_filenames.index(filename))},hidden=True),color='#00897',speed_multiplier=2,size=100,show_initially=False,fullscreenClassName="fullscreen-load",fullscreen=True),
                                                        dls.Hash(id={"type":"v-loading2","index":"v-"+str(u_filenames.index(filename))},children=html.Div(id={"type":"v-loading-output2","index":"v-"+str(u_filenames.index(filename))},hidden=True),color='#00897',speed_multiplier=2,size=100,show_initially=False,fullscreenClassName="fullscreen-load",fullscreen=True),
                                                        dcc.Download(id={"type":"v-download","index":"v-"+str(u_filenames.index(filename))}),
                                                        html.Div([html.Div(id={"type":"v-message","index":"v-"+str(u_filenames.index(filename))},className="alert alert-warning",hidden=True),
                                                                  html.Div(id={"type":"v-options","index":"v-"+str(u_filenames.index(filename))},className="v-options",hidden=True),
                                                                  html.Div([
                                                                            dbc.Modal([fac.AntdCarousel([html.Div([
                                                                                                         dbc.ModalHeader(dbc.ModalTitle(t),close_button=True),
                                                                                                         dbc.ModalBody([
                                                                                                                        html.Div([
                                                                                                                                  dash_table.DataTable(i.to_dict('records'),style_cell={'textAlign':'center','padding':'5px'},style_data={'color':'black','backgroundColor':'white'},style_header={'fontWeight':'bold','color':'white','backgroundColor':'#00897b'})
                                                                                                                                 ])
                                                                                                                       ]
                                                                                                                      )])for i,t in zip(issues_df,issues_title)
                                                                                                        ],dotPosition='top',id={"type":"v-carousel","index":"v-"+str(u_filenames.index(filename))}) 
                                                                                      ],id={"type":"v-modal","index":"v-"+str(u_filenames.index(filename))},size="xl",keyboard=False,backdrop="static",scrollable=True)
                                                                           ],id={"type":"v-table","index":"v-"+str(u_filenames.index(filename))},className="v-table",hidden=True)],id={"type":"v-info-block","index":"v-"+str(u_filenames.index(filename))},className="info-block",hidden=True)],
                                                        title=filename,id={"type":"v-button","index":"v-"+str(u_filenames.index(filename))},class_name="ic-acc-item"))
            if len(file_list)==0:
                return "",[],False,"Issues Detected",file_list,name_icon[1],True,True,new_style_loading,{"float":"right","display":"inline-block"},True,True,0,True,True,False
            else:
                return "",[],False,"Issues Detected",file_list,name_icon[1],True,True,new_style_loading,{"float":"right","display":"inline-block"},True,True,0,True,True,True
        else:
            raise PreventUpdate
            
    elif (page_id == name_icon[1]) & (submit_n_clicks is not None):
        
        try:
            if any("Predictions" in x for x in uploaded_filenames):
                dawpred_check = dawpred_upload(pd.read_excel("Predictions.xlsx"))
                print("Daw_Pred Done")
            if any("Master_Coding" in x for x in uploaded_filenames):
                aes_check = aes_upload(pd.read_excel("Master_Coding.xlsx"))
                print("Master_Coding Done")
            if any("Phase_In_Out" in x for x in uploaded_filenames):
                pio_check = pio_upload(pd.read_excel("Phase_In_Out.xlsx"))
                print("Phase_In_Out Done")
            if any("Closing_Sales" in x for x in uploaded_filenames):
                sales_check = sales_upload(pd.read_excel("Closing_Sales.xlsx"))
                print("Closing_Sales Done")
            if any("Opening_Stocks" in x for x in uploaded_filenames):
                stocks_check = stocks_upload(pd.read_excel("/Opening_Stocks.xlsx"))
                print("Opening_Stocks Done")
            if any("Production_Numbers" in x for x in uploaded_filenames):
                production_check = production_upload(pd.read_excel("Production_Numbers.xlsx"))
                print("Production_Numbers Done")
            
            count = 0
            for file in ["Production_Numbers.xlsx","Opening_Stocks.xlsx","Closing_Sales.xlsx","Phase_In_Out.xlsx","Master_Coding.xlsx","Predictions.xlsx"]:
                if (int(datetime.today().month) == int(datetime.fromtimestamp(os.path.getmtime(f"{file}")).month)):
                    count += 1
            if count == 6:
                print("############## ML_TRIGGERED ###############")
                subprocess.Popen([sys.executable,"automatic_call_LGBM.py"])
                
            return "",no_update,True,no_update,no_update,None,True,True,no_update,{"float":"right","display":"none"},True,False,0,True,True,True
        except:
            raise PreventUpdate
        
    else:
        raise PreventUpdate

#%%
# Validation Options
@callback(Output({'type':'v-options','index': MATCH},'hidden'),
          Output({'type':'v-options','index': MATCH},'children'),
          Output({'type':'v-message','index': MATCH},'hidden'),
          Output({'type':'v-message','index': MATCH},'children'),
          Output({'type':'v-info-block','index': MATCH},'hidden'),
          Output({'type':'v-loading-output','index': MATCH},'children'),
          Input("validation-file-list","active_item"),
          State({'type':'v-button','index': MATCH},'title'),
          State({'type':'v-button','index': MATCH},'id'),
          State("mapping-upload-data","filename"),
          prevent_initial_call = True)
def validation_options(active,filename,trig_id,uploaded_filenames):
    if active is not None:
        if int(active[-1]) == int(trig_id["index"][-1]):
            v_opt = dbc.RadioItems(id={"type":"v-opt","index":"v-"+str(active[-1])},
                                   className="btn-group",inputClassName="btn-check",labelClassName="btn btn-outline-info",
                                   labelCheckedClassName="active",options=[{"label":"View","value":0},{"label":"Download","value":1}],value=None)
            return False,html.Div(v_opt,id={"type":"v-opt-div","index":"v-"+str(active[-1])},className="v-opt-div"),False,html.Strong(["Please re-upload the files after making corrections!"]),False,""   
        else:
            raise PreventUpdate
    else:
        raise PreventUpdate

        
#%%
# Columns Mapping Dropdown       
@callback(Output({'type':'ic-columns-dropdown','index': MATCH},'options'),
          Output({'type':'ic-columns','index': MATCH},'hidden'),
          Output({'type':'ic-columns','index': MATCH},'children'),
          Output({'type':'div-ic-columns-dropdown','index': MATCH},'hidden'),
          Output({'type':'c-message','index': MATCH},'hidden'),
          Output({'type':'c-message','index': MATCH},'children'),
          Output({'type':'info-block','index': MATCH},'hidden'),
          Output({'type':'loading-output','index': MATCH},'children'),
          Output({'type':'select-button','index': MATCH},'style'),
          Input("mapping-file-list","active_item"),
          State({'type':'f-button','index': MATCH},'title'),
          State({'type':'f-button','index': MATCH},'id'),
          State("mapping-upload-data","filename"),
          prevent_initial_call = True)
def columns_mapping_dropdown(active,filename,trig_id,uploaded_filenames):
    
    if active is not None:
        if int(active[-1]) == int(trig_id["index"][-1]):
            
            if "i" in trig_id["index"]:
                if ".csv" in filename:
                    data = pd.read_csv(f"{filename}")
                elif '.xlsx' in filename:
                    data = pd.read_excel(f"{filename}")
                
                missing_columns = check_column_names(data,filename)
                if len(missing_columns) > 0:
                    m_col_lst = dbc.RadioItems(id={"type":"missing-columns","index":"ic-"+str(active[-1])},
                                               className="btn-group",inputClassName="btn-check",labelClassName="btn btn-outline-info",
                                               labelCheckedClassName="active",options=[{"label":col,"value":i} for i,col in enumerate(missing_columns)],value=None)
                    return list(data.columns),False,html.Div(m_col_lst,id={"type":"m-cols","index":"ic-"+str(active[-1])},className="m-cols"),False,True,html.Strong([]),False,"",{"display":"inline-block"}
                
                else:
                    return [],True,html.Div([],id={"type":"m-cols","index":"c-"+str(active[-1])},className="m-cols"),True,False,html.Strong(["All Columns Matched!"]),False,"",{"display":"none"}
            
            elif "i" not in trig_id["index"]:
                return [],True,html.Div([],id={"type":"m-cols","index":"c-"+str(active[-1])},className="m-cols"),True,False,html.Strong(["All Columns Matched!"]),False,"",{"display":"none"}
        else:
            raise PreventUpdate
    else:
        raise PreventUpdate
    

#%%
# Validation Options Selection
@callback(Output({'type':'v-modal','index': MATCH},'is_open'),
          Output({'type':'v-loading-output2','index': MATCH},'children'),
          Output({'type':'v-opt','index': MATCH},'value'),
          Output({'type':'v-download','index': MATCH},'data'),
          Input({'type':'v-opt','index': MATCH},'value'),
          State({'type':'v-opt','index': MATCH},'options'),
          State({'type':'v-carousel','index': MATCH},'children'),
          State({'type':'v-button','index': MATCH},'title'),
          prevent_initial_call = True)
def validation_options_selection(value,options,c_children,filename):
    if value is not None:
        if value == 0:
            return True,"",None,no_update
        elif value == 1:
            writer = pd.ExcelWriter(f'Issues_{filename}',engine='xlsxwriter')
            for ch in c_children:
                issue_name = ch["props"]["children"][0]["props"]["children"]["props"]["children"]
                data = pd.DataFrame(ch["props"]["children"][1]["props"]["children"][0]["props"]["children"][0]["props"]["data"])
                data.to_excel(writer,sheet_name=f'{issue_name}',index=False)
            writer.save()
            return False,"",None,dcc.send_file(f'Issues_{filename}')
            #issue_name = issue_name["props"]["children"]
            #data = pd.DataFrame(c_children[0]["props"]["data"])
            #return False,"",None,dcc.send_data_frame(data.to_excel,f"{issue_name}.xlsx",index=False)
        else:
            return False,"",None,no_update
    else:
        raise PreventUpdate
        
    

#%%
# Column Mapping Selection
@callback(Output({'type':'missing-columns','index': MATCH},'options'),
          Output({'type':'f-button','index': MATCH},'style'),
          Output({'type':'loading-output2','index': MATCH},'children'),
          Input({'type':'select-button','index': MATCH},'n_clicks'),
          State({'type':'missing-columns','index': MATCH},'options'),
          State({'type':'missing-columns','index': MATCH},'value'),
          State({'type':'ic-columns-dropdown','index': MATCH},'value'),
          State({'type':'f-button','index': MATCH},'title'),
          State("mapping-upload-data","filename"), 
          prevent_initial_call = True)
def columns_mapping_selection(n_clicks,options,m_value,d_value,filename,uploaded_filenames):
    
    if n_clicks > 0:
        
        if m_value is not None:
            if ".csv" in filename:
                file_contents = pd.read_csv(f"{filename}")
            elif '.xlsx' in filename:
                file_contents = pd.read_excel(f"{filename}")
            new_options = update_column([i["label"] for i in options if i["value"]==m_value][0],d_value,file_contents,options,int([options.index(i) for i in options if i["value"]==m_value][0]),filename)
            if len(new_options) == 0:
                return new_options,{"outline":"2px solid #50C878"},""
            else:
                return new_options,{"outline":"2px solid #FA8072"},""
        else:
            raise PreventUpdate
    else:
        raise PreventUpdate
        
#%%
# Proceed Button
@callback(Output('confirm-danger','displayed'),
          Input("proceed-button","n_clicks"),
          prevent_initial_call = True)
def proceed_button(n_clicks):
    if n_clicks > 0:
        return True
    else:
        return False


#%%



if __name__ == '__main__':
    app.run_server(debug=True,port=3000)
