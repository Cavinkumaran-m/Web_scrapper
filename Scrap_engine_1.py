import requests
from bs4 import BeautifulSoup
import shutil
import os
import base64
import sys
import mysql.connector
import datetime




####################### INTERNET CONNECTION CHECK ###############################
try:
    r = requests.get("https://www.google.com/")
except:
    print("No internet connectivity... Ending the program")
    sys.exit()



############################### SQL FUNCTIONS ####################################

connection = mysql.connector.connect(host='localhost', database='patch', user='root', password='')
cursor = connection.cursor()

def sql_close():
    cursor.close()
    connection.close()
    print("MySQL connection is closed")

def create_table(num):
    try:
        table_name = "page_" + str(num)
        command = "CREATE TABLE {} (PRODUCT_ID INT(10) UNIQUE, PRODUCT_NAME VARCHAR(255), IMAGE MEDIUMBLOB, DESCRIPTION TEXT(200));"
        cursor.execute(command.format(table_name));
        print("Table: ", table_name, " created!!")
    except:
        print("Table already created\n")
        


        
def insert_data(name_, id_, price_, desc_, bin_data, table_name, c):
    value_tuple = (id_, name_, bin_data, desc_)
    command = "INSERT INTO " + table_name + " (PRODUCT_ID, PRODUCT_NAME, IMAGE, DESCRIPTION) VALUES (%s, %s, %s, %s);"
    cursor.execute(command, value_tuple)
    print("Data upload: ", c, "/40")
    connection.commit()
    


############################# PROGRAM STARTS HERE ##############################
try:
    url = "https://www.popularpatch.com/military-patches?sort=newest&page="
    start = int(input("Enter starting page number: "))
    n = int(input("Enter number of Pages: "))
    end = start + n
    for i in range(start, end):
        r = requests.get(url + str(i))
        print(">>>WebsiteSuccessfully fetched\n")
        soup = BeautifulSoup(r.content, 'html.parser')
        page = soup.find('ul', attrs = { "class" : "productGrid"})

        folder_name = "images_" + str(i)
        try:
            os.mkdir(folder_name)
        except FileExistsError:
            print("File already created...")

        create_table(i)
        c = 0
        for patch in page.findAll('article', attrs = {'class' : 'card'}):
            mis_patch = patch.find('h4', attrs = {'class' : 'card-title'}).a.text
            patch_site = patch.find('h4', attrs = {'class' : 'card-title'}).a['href']
            patch_site_data = requests.get(patch_site)
            patch_site_html = BeautifulSoup(patch_site_data.content, 'html.parser')
            try:
                patch_name = patch_site_html.find('div', attrs = {'class' : 'productView-product'}).h1.text
            except:
                d = datetime.datetime.now()
                with open(r"C:\Users\User\Desktop\DATA\Web_Scrapper\engine_1_log.txt", 'a') as f:
                    f.write("\n\n" + table_name + "\n")
                    f.write(str(d) + ":\n")
                    f.write("Patch: " + str(mis_patch) + " not available on the internet!!!")
                continue
            patch_price = patch_site_html.find('span', attrs = {'class' : 'price price--withoutTax price--main'}).text
            patch_id = int(patch_site_html.find('dd', attrs = {'class' : 'productView-info-value productView-info-value--sku'}).text)
            patch_desc = str(patch_site_html.find('div', attrs = {'class' : 'productView-description-tabContent emthemesModez-mobile-collapse-content'}).text)
            patch_desc = patch_desc.strip()
            image_url = patch_site_html.find('li', {'class': 'productView-imageCarousel-main-item slick-current'}).a.get('href')
           
            filename =  folder_name + "\\" + str(patch_id) + ".jpg"
            img_obj = requests.get(image_url, stream = True)

            if img_obj.status_code == 200:
                img_obj.raw.decode_content = True
                
                with open(filename,'wb') as f:
                    shutil.copyfileobj(img_obj.raw, f)
                with open(filename, 'rb') as f:
                    BinaryData = f.read()
                

                table_name = "page_" + str(i)
                c += 1
                insert_data(patch_name, patch_id, patch_price, patch_desc, BinaryData, table_name, c)
                
            else:
                print("Status Code Error")
                
        shutil.rmtree(folder_name)

        
    sql_close()
    input()

except Exception as e:
    d = datetime.datetime.now()
    with open(r"C:\Users\User\Desktop\DATA\Web_Scrapper\engine_1_log.txt", 'a') as f:
        f.write("\n\n" + table_name + "\n")
        f.write(str(d) + ":\n")
        f.write(str(e))

