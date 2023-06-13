from bs4 import BeautifulSoup

def page_scraper_description(resHtml, hotelid, flag_description):
    # print('hello description!')
    result_description_upz = []
    checkin, checkout = '00:00:00', '00:00:00'
    try:
        soup1 = BeautifulSoup(resHtml, "lxml")  
        # print(soup1)
    except Exception as ex:
        # print(f"description____str19___{ex}")  
        return None    
    try: 
        checkin, checkout = checkin_checkout(soup1)
    except:
        pass

    if flag_description == True:
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
            return result_description_upz, checkin, checkout
        except Exception as ex:
            # print(f"description_func___str39___{ex}") 
            return None, checkin, checkout
    else:
        return None, checkin, checkout


def convert_to_24_hours(time_str):
    hour, minute = map(int, time_str[:-3].split(":"))
    period = time_str[-2:]

    if period == "PM" and hour < 12:
        hour += 12
    elif period == "AM" and hour == 12:
        hour = 0

    return f"{hour:02d}:{minute:02d}:00"


def checkin_checkout(soup1):
    try:
        checkin = soup1.find('div', attrs={'id': 'checkin_policy'}).find_all('p')[1].get_text().strip().split(' ')[-1].strip() 
        checkout = soup1.find('div', attrs={'id': 'checkout_policy'}).find_all('p')[1].get_text().strip().split(' ')[-1].strip()   
    except:
        checkin = '14:00'
        checkout = '12:00'
    try:
        checkin = convert_to_24_hours(checkin) 
        checkout = convert_to_24_hours(checkout) 
    except:
        checkin = '14:00'
        checkout = '12:00'
    
    return checkin, checkout



