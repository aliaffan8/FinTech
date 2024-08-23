from openai import OpenAI
import pdfplumber
import json
import pandas as pd
import glob
from api_keys import openai_key
import re
from azure_ocr import read_ocr
from prompts import *
import threading
import shutil, os


client = OpenAI(api_key=openai_key)
f = open('patterns.json')
patterns = json.load(f)
f.close()



def get_completion(prompt,model="gpt-3.5-turbo-1106",response_format="json_object"):
  response = client.chat.completions.create(
    model=model,
    response_format = {"type":response_format},
    messages=[
      {"role": "user", "content":prompt}
    ]
  )
  return response.choices[0].message.content

def read_file(file_path,f):
    if file_path[-3:]=="pdf":
        with pdfplumber.open(file_path) as pdf:
            text = ""
            pages = []
            for page in pdf.pages:
                p = page.extract_text()
                p = re.sub(r',', '', p)
                text += p
                pages.append(p)
        with open(f'txts/{f}.txt',"w", encoding="utf-8",)as x:
            x.write(text)
	
        return text, pages
    elif file_path[-3:]=="txt":
        with open(file_path) as tx:
            lines = tx.readlines()
        pages = []
        for i in range(0, len(lines), 50):
            page = '\n'.join(lines[i:i+50])
            pages.append(page)
        return '\n'.join(lines), pages
    
def validate_next(pages):
  prev_balance = 0
  dfs = []
  for p in range(len(pages)):
    df = pages[p]
    df.fillna(0.0, inplace=True)
    for i, row in df.iterrows():
      b = str(row.iloc[4])
      db = str(df.iloc[i-1,2])
      cr = str(df.iloc[i-1,3])
      if db == '-':
        db = '0.0'
        df.iloc[i-1,2] = '0.0'
      if cr == '-':
        cr = '0.0'
        df.iloc[i-1,3] = '0.0'
      if float(b.replace(' ','')) != round(prev_balance + float(cr.replace(' ','')) - float(db.replace(' ','')),2) and i>0:
        if prev_balance == float(b.replace(' ','')):
          with open('error.txt','a') as fe:
            fe.write('Validate | Same balance as before\n' + f"index {i} | {row} \n\n prev: {df.iloc[i-1,:]}")
        if float(b.replace(' ','')) != round(prev_balance - float(cr.replace(' ','')) + float(db.replace(' ','')),2)  :
          missed += 1
          df.iloc[i,-1] = '<--'
          df.iloc[i-1,-1] = '<--'
          with open('error.txt','a') as fe:
            fe.write(f"Validate page = {p} - index = {i-1} -> {i}| Inconsistency error: Missing Transactions between previous row: \n{df.iloc[i-1]} \nANd\n {row}\n")
            fe.write(f"index {i} | {float(b.replace(' ',''))} != {prev_balance} + {float(cr.replace(' ',''))} -{ float(db.replace(' ',''))} = { round(prev_balance + float(cr.replace(' ','')) - float(db.replace(' ','')),2)}\n")
            fe.write(f"{float(b.replace(' ',''))} != {prev_balance} - {float(cr.replace(' ',''))} +{ float(db.replace(' ',''))} = { round(prev_balance - float(cr.replace(' ','')) + float(db.replace(' ','')),2)}\n\n")
          # raise BaseException(f"Inconsistency error: Missing Transactions between previous row: {df.iloc[i-1]} \nANd\n {row}")
        else:
          temp = df.iloc[i,2]
          df.iloc[i,2] = df.iloc[i,3]
          df.iloc[i,3] = temp
      if float(db.replace(' ','')) < 0:
          df.iloc[i,2] = str(df.iloc[i,3])[1:]
          df.iloc[i,3] = '0.0'
      if float(cr.replace(' ','')) < 0 :
          df.iloc[i,2] = str(df.iloc[i,2])[1:]
      prev_balance = float(b.replace(' ',''))
    dfs.append(df)
  concatenated_df = pd.concat(dfs)
  concatenated_df.reset_index(drop=True, inplace=True)
  return concatenated_df

def validate(pages):
  prev_balance = 85311.37 #float(pages[0]['opening_balance'])
  dfs = []
  missed = 0
  for p in range(len(pages)):
    df = pages[p]
    df.fillna(0.0, inplace=True)
    for i, row in df.iterrows():
      b = str(row.iloc[4])
      db = str(row.iloc[2])
      cr = str(row.iloc[3])
      if float(b.replace(' ','')) != round(prev_balance + float(cr.replace(' ','')) - float(db.replace(' ','')),2):
        if prev_balance == float(b.replace(' ','')):
          with open('error.txt','a') as fe:
            fe.write(f'Validate page = {p} - index = {i-1} -> {i}| Same balance as before\n' + f"index {i} | {row} \n\n prev: {df.iloc[i-1,:]}")
        if float(b.replace(' ','')) != round(prev_balance - float(cr.replace(' ','')) + float(db.replace(' ','')),2)  and i >0:
          missed += 1
          df.iloc[i,-1] = '<--'
          df.iloc[i-1,-1] = '<--'
          with open('error.txt','a') as fe:
            fe.write(f"Validate page = {p} - index = {i-1} -> {i}| Inconsistency error: Missing Transactions between previous row: \n{df.iloc[i-1]} \nANd\n {row}\n")
            fe.write(f"index {i} | {float(b.replace(' ',''))} != {prev_balance} + {float(cr.replace(' ',''))} -{ float(db.replace(' ',''))} = { round(prev_balance + float(cr.replace(' ','')) - float(db.replace(' ','')),2)}\n")
            fe.write(f"{float(b.replace(' ',''))} != {prev_balance} - {float(cr.replace(' ',''))} +{ float(db.replace(' ',''))} = { round(prev_balance - float(cr.replace(' ','')) + float(db.replace(' ','')),2)}\n\n")
          # raise BaseException(f"Inconsistency error: Missing Transactions between previous row: {df.iloc[i-1]} \nANd\n {row}")
        else:
          temp = df.iloc[i,2]
          df.iloc[i,2] = df.iloc[i,3]
          df.iloc[i,3] = temp
      if float(db.replace(' ','')) < 0:
          df.iloc[i,2] = str(df.iloc[i,3])[1:]
          df.iloc[i,3] = '0.0'
      if float(cr.replace(' ','')) < 0 :
          df.iloc[i,2] = str(df.iloc[i,2])[1:]
        
      prev_balance = float(b.replace(' ',''))
    dfs.append(df)
  concatenated_df = pd.concat(dfs)
  concatenated_df.reset_index(drop=True, inplace=True)
  return concatenated_df, missed

def get_completion_concur(p, prompt, result,lock,model="gpt-3.5-turbo-1106",response_format="json_object"):
    for k in range(3):
      try:
        response = client.chat.completions.create(
        model=model,
        # response_format = {"type":response_format},
        messages=[
          {"role": "user", "content":prompt}
          ]
        )
        r = response.choices[0].message.content
        with open(f'responses/{p}.txt','w') as fa:
          fa.write(r)
        t = pd.read_csv(f'responses/{p}.txt')
        return
      except Exception as e:
         print(f'error: {e} | pg {p}, \nresponse :{r} ')
         print(f'Try number { k+1}')
    raise Exception(f'Failed to get JSON response after 3 tries for page : {p}')
        

def get_completion_handler(pages,prompt):
    threads = []
    results = {}
    lock = threading.Lock()
    shutil.rmtree('responses/')
    os.mkdir('responses/')
    for p in range(len(pages)):
      pg = pages[p]
      t = prompt.format(text=pg)
      t = threading.Thread(target=get_completion_concur, args=(p,t, results, lock))
      threads.append(t)
      t.start()
            

    # Wait for all threads to complete
    for t in threads:
        t.join()
    # Results are ready to be concatenated/used
    files = len(glob.glob('responses/*.txt'))
    rsps = []
    for i in range(files):
        df = pd.read_csv(f'responses/{i}.txt')
        df['inconsistency'] = ''
        df = df.replace('-', 0.0)
        df = df.replace(' ', 0.0)
        rsps.append(df)
    
    return rsps, None
  
def process_group2(fil,f):
	first_page = 0
	text, pages = read_file(fil,f)
	rsps = []
	fp = True
	for p in range(first_page,len(pages)):
		pg = pages[p]
		if fp:
			t = prompt.format(text=pg)
			fp = False
		else:
			t = prompt_second_page.format(text=pg)
		r = get_completion(t)
		r = re.sub(r'(?<=\d),(?=\d)', '', r)
		try:
			rsp = json.loads(r) 
		except Exception as e:
			with open('error.txt','a') as fe:
				fe.write(f'JSON LOAD ERROR {f} | {e} | {r}\n\n')
			print(f'JSON LOAD ERROR {f} | {e} | {r}')
			continue
		rsps.append(rsp)
	with open(f'jsons/{f}-group2.json',"w", encoding="utf-8")as x:
		json.dump(rsps, x, indent=4)
	res = validate_next(rsps)
	res.to_csv(f'csvs/{f}-group2.csv'); return res
  
  

def process_group0_concur(fil,f):
  first_page = 0
  try:
    text, pages = read_ocr(fil,f)
  except Exception as e:
    with open('error.txt','a') as fe:
      fe.write(f'OCR READING ERROR {f} | {e} \n\n')
    print(f'OCR READING ERROR {f} | {e} ')
    return
  print("Getting Response")
  for j in range(3):
    try:
      rsps, error = get_completion_handler(pages,prompt_column2)
      if error is not None:
          with open('error.txt','a') as fe:
            fe.write(f'JSON LOAD ERROR {f} | {error} \n\n')
          print(f'JSON LOAD ERROR {f} | {error} ')
          return
      # with open(f'jsons/{f}-group4.json',"w", encoding="utf-8")as x:
      #   json.dump(rsps, x, indent=4)
      tot = 0
      print('lengths 24 28 27 19')
      for i in rsps:
          print(len(i))
          tot += len(i)
      res, missed = validate(rsps)
      print(f'Extracted {tot} | missed {missed}')
      with open('error.txt','a') as fe:
        fe.write(f'\n\n Total extracted {tot} | missed {missed}\n\n')
      res.to_csv(f'csvs/{f}-group4.csv')
      break
    except Exception as e:
      print(f'Overall try {j+2} | {f} | {e}')
      with open('error.txt','a') as fe:
        fe.write(f'Overall Retry {j+2} | {f} | {e}\n')
  return res

def process_group1_concur(fil,f):
  first_page = 0
  try:
    text, pages = read_ocr(fil,f)
  except Exception as e:
    with open('error.txt','a') as fe:
      fe.write(f'OCR READING ERROR {f} | {e} \n\n')
    print(f'OCR READING ERROR {f} | {e} ')
    return
  print("Getting Response")
  for j in range(3):
    try:
      rsps, error = get_completion_handler(pages,prompt_table)
      if error is not None:
          with open('error.txt','a') as fe:
            fe.write(f'JSON LOAD ERROR {f} | {error} \n\n')
          print(f'JSON LOAD ERROR {f} | {error} ')
          return
      # with open(f'jsons/{f}-group4.json',"w", encoding="utf-8")as x:
      #   json.dump(rsps, x, indent=4)
      tot = 0
      # print('lengths 24 28 27 19')
      for i in rsps:
          # print(len(i))
          tot += len(i)
      res, missed = validate(rsps)
      print(f'Extracted {tot} | missed {missed}')
      with open('error.txt','a') as fe:
        fe.write(f'\n\n Total extracted {tot} | missed {missed}\n\n')
      res.to_csv(f'csvs/{f}-group4.csv')
      break
    except Exception as e:
      print(f'Overall try {j+2} | {f} | {e}')
      with open('error.txt','a') as fe:
        fe.write(f'Overall Retry {j+2} | {f} | {e}\n')
  return res

def process_pdfs_txts():
    open('error.txt','w').close()
    files = glob.glob("./pdfs/*")
    file_path = 'csvs/output.xlsx'

    # Create a Pandas Excel writer
    writer =  pd.ExcelWriter(file_path)
    for fy in range(len(files)):
        fil = files[fy]
        f = fil[7:-4]
        key = "_".join(f.split('_')[-2:])
        with open('error.txt','a') as fe:
          fe.write(f"############ {patterns[key]['group']} - {f} ###########\n\n")
        
        if patterns[key]['group'] == "Group0":
            print(f"############ Group0 - {f} ###########")
            res = process_group0_concur(fil,f)

        # elif patterns[key]['group'] == "Group1":
        #     print(f"############ Group1 - {f} ###########")
        #     process_group1(fil,f)
        
        # elif patterns[key]['group'] == "Group2":
        #     print(f"############ Group2 - {f} ###########")
        #     process_group2(fil,f)
        
        # elif patterns[key]['group'] == "Group3":
        #     print(f"############ Group3 - {f} ###########")
        #     process_group3_concur(fil,f)
        
        # elif patterns[key]['group'] == "Group4":
        #     print(f"############ Group4 - {f} ###########")
        #     process_group0_concur(fil,f)
        
        else: 
            print(f"############ {f} ###########")
            with open('error.txt','a') as fe:
              fe.write(f"Unable to process yet\n")
            print("unable to process type yet",)
            res = process_group0_concur(fil,f)
        try:
          res.to_excel(writer, sheet_name=f, index=False)
        except Exception as e:
          with open('error.txt','a') as fe:
              fe.write(f'excel error {e}, {f}')
          print(f'excel error {e}, {f}')
    writer.close()    
if __name__ == "__main__":
  process_pdfs_txts()
            