def db_wrtr(total, n2):
    try:
        import time
        import mysql.connector
        from mysql.connector import connect, Error 
        from . import config_real
        print('hello db_writerr')

        config = {
            'user': config_real.user,
            'password': config_real.password,
            'host': config_real.host,
            'port': config_real.port,
            'database': config_real.database,      
        }
        for _ in range(3):
            try:
                conn = mysql.connector.connect(**config)      
                print("Writerr connection established description")
                break
            except Error as e:
                print(f"Error connecting to MySQL: {e}")
                time.sleep(3)
                continue
            
        try:
            cursor = conn.cursor() 
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
                # time.sleep(3)
        try:
            print(len(total))
            resDescription = []          
            whiteList = []
            n = int(int(n2)/1000)
        except:
            pass
        try:
            for t in total:
                try:
                    resDescription += t[0]
                except Exception as ex:
                    continue

            resDescription = list(filter(None, resDescription))
            resDescription = remove_repetitions(resDescription)
            print(f"len_description___{len(resDescription)}")
        except:
            pass       

        try:
            whiteList = writerr_table(conn, cursor, resDescription)
        except:
            pass

        try:
            if len(whiteList) != 0:
               changing_hotelsCritery(cursor, conn, whiteList)
        except:
            pass

        try:
            checkin_checkout_updated(cursor, conn, resDescription)
        except:
            pass

        try:
           semaforr(conn, cursor, n)
        except:
            pass
 
        try:
            cursor.close()
            conn.close()
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
    except:
        pass
    return


def writerr_table(conn, cursor, resDescription):
    descr_white = []
    descr_white_add = []
    descr_white_set = set()
    descr_white_batch_set = set()

    try:
        query2 = "INSERT INTO upz_hotels_description (hotelid, runame, dename, frname, enusname, esname, ptptname, itname, trname, arname, zhcnname, idname) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        batch_size = 250
        batch_values = []

        for item in resDescription:
            try:
                values = (item["hotelid"], item["runame"], item["dename"], item["frname"], item["enusname"], item["esname"], item["ptptname"], item["itname"], item["trname"], item["arname"], item["zhcnname"], item["idname"])
                batch_values.append(values)
                descr_white_batch_set.add(item["hotelid"])

                if len(batch_values) >= batch_size:
                    try:
                        cursor.executemany(query2, batch_values)
                        conn.commit()
                        descr_white_set.update(descr_white_batch_set)
                        descr_white_batch_set =set()
                        batch_values = []
                    except Exception as ex:
                        print(f"117___{ex}")                        
                        descr_white_add = insert_rows_individually_descr(conn, cursor, query2, batch_values)
                        descr_white += descr_white_add
                        descr_white_batch_set = set()
                        batch_values = []
                        continue                   

            except Exception as ex:
                print(f"122___{ex}")
                continue

        if batch_values:
            try:
                cursor.executemany(query2, batch_values)
                conn.commit()
                descr_white_set.update(descr_white_batch_set)
            except Exception as ex:
                # print(f"130___{ex}")
                descr_white_add = insert_rows_individually_descr(conn, cursor, query2, batch_values)
                descr_white += descr_white_add
                descr_white_batch_set = set()
    except Exception as ex:
        print(f"123___{ex}")
        pass

    try:
        descr_white += list(descr_white_set)
    except Exception as ex:
        print(ex)

    return descr_white


def insert_rows_individually_descr(conn, cursor, query, data):
    descr_white_set = set()
    descr_white = []
    try:
        data = eval(data)
    except:
        data = data
    for item in data:
        try:
            values = item
            cursor.execute(query, values)            
            descr_white_set.add(item[0])
        except Exception as ex:
            # print(f"204___: {ex}")
            continue
    try:
        conn.commit()
    except:
        descr_white = []
        return descr_white
    descr_white = list(descr_white_set)
    return descr_white

def changing_hotelsCritery(cursor, conn, whiteList):
    try:       
        query12 = "UPDATE upz_hotels SET description = %s WHERE hotel_id = %s"

        for item in whiteList:
            try:
                try:
                    find_query = "SELECT hotel_id FROM upz_hotels WHERE hotel_id = %s"                     
                    cursor.execute(find_query, (item,))                      
                    row = cursor.fetchone()                      
                except Exception as ex:
                    print(f"db_writerr__str87__{ex}")

                if row:
                    try:                      
                        cursor.execute(query12, (1, item))                      
                    except Exception as ex:
                        print(f"db_writerr__str90__{ex}")

            except Exception as ex:
                print(f"db_writerr__str95__{ex}")
                continue
        conn.commit()           

    except Exception as ex:
        print(ex)

    return

def checkin_checkout_updated(cursor, conn, resDescription):
    import datetime
    try:
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        query9 = "UPDATE upz_hotels SET checkin = %s, checkout = %s, updated = %s WHERE hotel_id = %s"

        for item in resDescription:
            try:
                try:
                    find_query = "SELECT hotel_id FROM upz_hotels WHERE hotel_id = %s"                     
                    cursor.execute(find_query, (item["hotelid"],))                      
                    row = cursor.fetchone()                      
                except Exception as ex:
                    print(f"db_writerr__str87__{ex}")

                if row:
                    try:                      
                        cursor.execute(query9, (item["checkin"], item["checkout"], current_date, item["hotelid"]))                      
                    except Exception as ex:
                        print(f"db_writerr__str90__{ex}")

            except Exception as ex:
                print(f"db_writerr__str95__{ex}")
                continue
        conn.commit()           

    except Exception as ex:
        print(ex)


def semaforr(conn, cursor, n):  
    print(n)
    try:
        select_queryF = "SELECT deskription_flag FROM hotels_simafor WHERE id = %s"
        cursor.execute(select_queryF, (n,))
        row = cursor.fetchone()
        # print('helo simafor')
        if row:
            update_query = "UPDATE hotels_simafor SET deskription_flag = %s WHERE id = %s"
            cursor.execute(update_query, (1, n))
            conn.commit()
    except Exception as ex:
        print(ex)

    return 

def remove_repetitions(data):
    unique_values = set()
    result = []
    for item in data:
        unil_value = item.get("hotelid")
        if unil_value not in unique_values:
            result.append(item)
            unique_values.add(unil_value)
    return result
