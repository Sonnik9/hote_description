from bs4 import BeautifulSoup

def page_scraper_description(resHtml, hotelid):
    # print('hello description!')
    result_description_upz = []

    try:
        soup1 = BeautifulSoup(resHtml, "lxml")  
        # print(soup1)
    except Exception as ex:
        # print(f"description____str19___{ex}")  
        return None

    try:
        description = ''      
        description_block = soup1.find("div", attrs={"class": "hp_desc_main_content"})
        description = description_block.get_text(strip=True, separator="\n")  

        result_description_upz.append({
            "id": "",
            "hotelid": int(hotelid),
            "runame": '',
            "dename": '',
            "frname": '',
            "enusname": str(description),
            "esname": '',
            "ptptname": '',
            "itname": '',
            "trname": '',
            "arname": '',
            "zhcnname": '',
            "idname": ''
        })

    except Exception as ex:
        # print(f"description_func___str32___{ex}") 
        # pass
        return None 

    try:
        return result_description_upz
    except Exception as ex:
        # print(f"description_func___str39___{ex}") 
        return None
