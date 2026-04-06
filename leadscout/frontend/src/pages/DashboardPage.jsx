import React from "react";
import { useEffect, useMemo, useRef, useState } from "react"
import { useNavigate } from "react-router-dom"
import Nav from "../components/Nav"
import SparklesBg from "../components/SparklesBg"
import { authFetch } from "../lib/auth"

const WS_BASE = `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.host}/api`
const LIVE_FEED_MAX_ROWS = 200
const LIVE_FEED_FLUSH_MS = 700

// ── Complete World Database ─────────────────────────────
const WORLD = {
  Afghanistan: { Kabul: ["Kabul City Centre","Wazir Akbar Khan","Shahr-e-Naw","Macroyan"] },
  Albania: { Tirana: ["Tirana City Centre","Blloku","Kombinat"] },
  Algeria: { Algiers: ["Algiers City Centre","Bab El Oued","El Biar","Hydra","Ben Aknoun"] },
  Argentina: {
    "Buenos Aires": ["Palermo","Belgrano","Recoleta","San Telmo","Caballito","Flores","Almagro","Villa Crespo"],
    Cordoba: ["Nueva Cordoba","General Paz","Alberdi"],
    Rosario: ["Rosario City Centre","Echesortu"],
  },
  Australia: {
    Sydney: ["CBD","Parramatta","Bondi Junction","Chatswood","Liverpool","Blacktown","Campbelltown","Hornsby","Hurstville","Bankstown","Castle Hill","Manly","Cronulla","Burwood","Strathfield","Ryde","Miranda","Sutherland"],
    Melbourne: ["CBD","St Kilda","Richmond","Fitzroy","Brunswick","Footscray","Dandenong","Box Hill","Doncaster","Ringwood","Frankston","Werribee","Sunshine","Springvale","Cranbourne","Moonee Ponds","Essendon","Preston","Coburg","Northcote"],
    Brisbane: ["CBD","South Brisbane","Fortitude Valley","New Farm","West End","Indooroopilly","Chermside","Mt Gravatt","Logan","Ipswich","Toowoomba","Wynnum","Carindale"],
    Perth: ["CBD","Fremantle","Joondalup","Rockingham","Mandurah","Midland","Armadale","Canning Vale","Morley","Belmont","Cannington"],
    Adelaide: ["CBD","Glenelg","Port Adelaide","Norwood","Marion","Salisbury","Mount Barker","Modbury"],
    "Gold Coast": ["Surfers Paradise","Broadbeach","Robina","Southport","Coolangatta","Nerang"],
    Canberra: ["Canberra City","Belconnen","Woden","Tuggeranong","Gungahlin"],
    Darwin: ["Darwin City","Palmerston","Casuarina"],
    Hobart: ["Hobart City","Glenorchy","Clarence"],
  },
  Austria: {
    Vienna: ["Vienna Innere Stadt","Vienna Mariahilf","Vienna Favoriten","Vienna Floridsdorf","Vienna Meidling","Vienna Penzing","Vienna Hernals"],
    Graz: ["Graz City Centre","Graz Liebenau","Graz Eggenberg"],
    Salzburg: ["Salzburg City Centre","Salzburg Maxglan"],
    Linz: ["Linz City Centre","Linz Urfahr"],
    Innsbruck: ["Innsbruck City Centre","Innsbruck Pradl"],
  },
  Bahrain: {
    Manama: ["Seef District","Adliya","Juffair","Zinj","Sanabis","Tubli","Amwaj Islands"],
    Riffa: ["Riffa East","Riffa West"],
    "Other Bahrain": ["Hamad Town","Muharraq","Busaiteen","Isa Town","Galali"],
  },
  Bangladesh: {
    Dhaka: ["Gulshan","Banani","Dhanmondi","Uttara","Motijheel","Mirpur","Mohammadpur"],
    Chittagong: ["Chittagong City Centre","Agrabad","Nasirabad"],
    Sylhet: ["Sylhet City Centre","Zindabazar"],
  },
  Belgium: {
    Brussels: ["Brussels City Centre","Brussels Ixelles","Brussels Molenbeek","Brussels Schaerbeek","Brussels Etterbeek","Brussels Anderlecht"],
    Antwerp: ["Antwerp City Centre","Antwerp Borgerhout","Antwerp Berchem"],
    Ghent: ["Ghent City Centre","Ghent Ledeberg"],
    Bruges: ["Bruges City Centre"],
    Liège: ["Liège City Centre"],
  },
  Brazil: {
    "São Paulo": ["Paulista","Jardins","Pinheiros","Vila Madalena","Moema","Itaim Bibi","Santo André","São Bernardo"],
    "Rio de Janeiro": ["Copacabana","Ipanema","Leblon","Barra da Tijuca","Botafogo","Flamengo"],
    Brasília: ["Asa Norte","Asa Sul","Taguatinga","Ceilandia"],
    Salvador: ["Barra","Ondina","Pituba","Federação"],
    Fortaleza: ["Meireles","Aldeota","Varjota"],
  },
  Canada: {
    Toronto: ["Downtown Toronto","Scarborough","Mississauga City Centre","Brampton","Markham","North York","Etobicoke","Richmond Hill","Vaughan","Oakville","Burlington","Hamilton","Pickering","Whitby","Oshawa","Barrie"],
    Vancouver: ["Downtown Vancouver","Surrey City Centre","Surrey Newton","Burnaby Metrotown","Richmond BC","Coquitlam","Langley","Abbotsford","North Vancouver","West Vancouver","Delta","New Westminster"],
    Calgary: ["Downtown Calgary","Northwest Calgary","Northeast Calgary","Southeast Calgary","Southwest Calgary","Airdrie","Cochrane"],
    Montreal: ["Downtown Montreal","Plateau Mont Royal","Rosemont","Hochelaga","Laval","Longueuil","Saint Laurent","Lasalle","Verdun","Anjou"],
    Ottawa: ["Downtown Ottawa","Gatineau","Orleans","Kanata","Barrhaven","Nepean"],
    Edmonton: ["Downtown Edmonton","West Edmonton","South Edmonton","North Edmonton","St Albert","Sherwood Park"],
    Winnipeg: ["Downtown Winnipeg","St Vital","Transcona","St James","Polo Park"],
  },
  Chile: {
    Santiago: ["Las Condes","Providencia","Miraflores","San Miguel","Santiago Centro","Vitacura","Ñuñoa"],
    Valparaíso: ["Valparaíso City","Viña del Mar","Concón"],
    Concepción: ["Concepción City","Talcahuano"],
  },
  China: {
    Beijing: ["Chaoyang","Haidian","Dongcheng","Xicheng","Fengtai","Shijingshan"],
    Shanghai: ["Pudong","Jing'an","Huangpu","Xuhui","Changning","Putuo"],
    Guangzhou: ["Tianhe","Yuexiu","Haizhu","Liwan","Baiyun"],
    Shenzhen: ["Futian","Nanshan","Luohu","Yantian","Longhua"],
    Chengdu: ["Jinjiang","Chenghua","Qingyang","Wuhou"],
  },
  Colombia: {
    Bogotá: ["Chapinero","Usaquén","Suba","Kennedy","Engativá","Fontibón"],
    Medellín: ["El Poblado","Laureles","Envigado","Bello","Sabaneta"],
    Cali: ["El Peñón","Granada","Ciudad Jardín","Chipichape"],
  },
  "Czech Republic": {
    Prague: ["Prague Old Town","Prague Vinohrady","Prague Zizkov","Prague 6 Dejvice","Prague 7 Holesovice","Prague Nusle"],
    Brno: ["Brno City Centre","Brno Zabovresky"],
    Ostrava: ["Ostrava City Centre","Poruba"],
  },
  Denmark: {
    Copenhagen: ["Copenhagen Indre By","Copenhagen Nørrebro","Copenhagen Vesterbro","Copenhagen Østerbro","Copenhagen Frederiksberg"],
    Aarhus: ["Aarhus City Centre","Aarhus V","Aarhus N"],
    Odense: ["Odense City Centre"],
    Aalborg: ["Aalborg City Centre"],
  },
  Egypt: {
    Cairo: ["Zamalek","Maadi","Heliopolis","Nasr City","New Cairo","6th of October","Dokki","Mohandessin","Giza"],
    Alexandria: ["Smouha","Miami Alexandria","Stanley","Sidi Gaber","Laurent"],
    Hurghada: ["Hurghada City","El Dahar","Sakalla"],
  },
  Finland: {
    Helsinki: ["Helsinki Kamppi","Helsinki Kallio","Helsinki Töölö","Helsinki Pasila","Helsinki Espoo Tapiola"],
    Tampere: ["Tampere City Centre","Tampere Hervanta"],
    Turku: ["Turku City Centre","Turku Kupittaa"],
    Oulu: ["Oulu City Centre"],
  },
  France: {
    Paris: ["Paris Marais","Paris Montmartre","Paris La Defense","Paris Bastille","Paris Belleville","Paris Nation","Paris Montparnasse","Paris Pigalle","Paris Latin Quarter","Paris Batignolles","Paris Saint-Germain"],
    Lyon: ["Lyon Part Dieu","Lyon Confluence","Lyon Croix Rousse","Lyon Presquile","Lyon Villeurbanne"],
    Marseille: ["Marseille Prado","Marseille Castellane","Marseille Timone","Marseille Old Port"],
    Toulouse: ["Toulouse Capitole","Toulouse Compans","Toulouse Minimes"],
    Nice: ["Nice Promenade","Nice Cimiez","Nice Liberation"],
    Bordeaux: ["Bordeaux City Centre","Bordeaux Chartrons","Bordeaux Mériadeck"],
    Strasbourg: ["Strasbourg City Centre","Strasbourg Cronenbourg"],
    Nantes: ["Nantes City Centre","Nantes Erdre"],
    Lille: ["Lille City Centre","Lille Vieux-Lille","Roubaix","Tourcoing"],
  },
  Germany: {
    Berlin: ["Berlin Mitte","Berlin Charlottenburg","Berlin Kreuzberg","Berlin Prenzlauer Berg","Berlin Neukolln","Berlin Schoneberg","Berlin Spandau","Berlin Marzahn","Berlin Tempelhof","Berlin Steglitz"],
    Munich: ["Munich City Centre","Munich Schwabing","Munich Maxvorstadt","Munich Bogenhausen","Munich Neuhausen","Munich Pasing","Munich Sendling"],
    Hamburg: ["Hamburg City Centre","Hamburg Altona","Hamburg Eimsbüttel","Hamburg Harburg","Hamburg Wandsbek","Hamburg Barmbek","Hamburg Bergedorf"],
    Frankfurt: ["Frankfurt City Centre","Frankfurt Sachsenhausen","Frankfurt Bornheim","Frankfurt Westend","Frankfurt Nordend","Frankfurt Bockenheim"],
    Cologne: ["Cologne City Centre","Cologne Ehrenfeld","Cologne Nippes","Cologne Deutz","Cologne Mulheim"],
    Düsseldorf: ["Düsseldorf Altstadt","Düsseldorf Pempelfort","Düsseldorf Flingern"],
    Stuttgart: ["Stuttgart City Centre","Stuttgart Vaihingen","Stuttgart Zuffenhausen"],
    Leipzig: ["Leipzig City Centre","Leipzig Connewitz","Leipzig Plagwitz"],
    Dresden: ["Dresden City Centre","Dresden Neustadt"],
    Nuremberg: ["Nuremberg City Centre","Nuremberg Gostenhof"],
    Bremen: ["Bremen City Centre","Bremen Schwachhausen"],
    Hanover: ["Hanover City Centre","Hanover Linden"],
    Dortmund: ["Dortmund City Centre","Dortmund Innenstadt-Nord"],
  },
  Greece: {
    Athens: ["Kolonaki","Glyfada","Kifissia","Exarcheia","Piraeus","Marousi","Paleo Faliro"],
    Thessaloniki: ["Thessaloniki City Centre","Kalamaria","Stavroupoli"],
    Heraklion: ["Heraklion City","Heraklion Old Town"],
  },
  India: {
    Mumbai: ["Andheri West","Andheri East","Bandra West","Bandra East","Borivali","Dadar","Thane West","Thane East","Kurla","Malad West","Malad East","Goregaon","Kandivali","Mulund","Powai","Worli","Colaba","Chembur","Ghatkopar","Vikhroli","Juhu","Santacruz","Vile Parle","Navi Mumbai Vashi","Navi Mumbai Kharghar","Navi Mumbai Belapur","Kalyan","Dombivli","Panvel","Vasai","Mira Road"],
    Delhi: ["Connaught Place","Karol Bagh","Lajpat Nagar","Dwarka Sector 7","Dwarka Sector 10","Dwarka Sector 14","Rohini","Pitampura","Janakpuri","Saket","Vasant Kunj","Vasant Vihar","Greater Kailash","Nehru Place","Preet Vihar","Shahdara","Dilshad Garden","Mayur Vihar Phase 1","Mayur Vihar Phase 2","Patel Nagar","Rajouri Garden","Uttam Nagar","Paschim Vihar","Punjabi Bagh","Malviya Nagar","Hauz Khas","Green Park","Defence Colony","Noida Sector 18","Noida Sector 62","Noida Sector 50","Noida Sector 137","Greater Noida","Gurgaon DLF Phase 1","Gurgaon DLF Phase 2","Gurgaon Sector 29","Gurgaon Sohna Road","Gurgaon Golf Course Road","Gurgaon MG Road","Faridabad Sector 15","Ghaziabad Vaishali","Ghaziabad Indirapuram"],
    Bangalore: ["Koramangala 1st Block","Koramangala 4th Block","Koramangala 8th Block","Indiranagar 1st Stage","Indiranagar 2nd Stage","Whitefield","Electronic City Phase 1","Electronic City Phase 2","HSR Layout","BTM Layout","Jayanagar","JP Nagar","Bannerghatta Road","Marathahalli","Sarjapur Road","Bellandur","Hebbal","Yelahanka","Malleshwaram","Rajajinagar","Basavanagudi","Vijayanagar","Banashankari","Kengeri","MG Road","Brigade Road","Richmond Town","Ulsoor","CV Raman Nagar","KR Puram","Bommanahalli","Domlur","Nagarbhavi","Hennur Road","Banaswadi"],
    Hyderabad: ["Jubilee Hills","Banjara Hills Road 1","Banjara Hills Road 12","Hitech City","Gachibowli","Madhapur","Kondapur","Kukatpally","Begumpet","Secunderabad","Ameerpet","SR Nagar","Somajiguda","Punjagutta","Dilsukhnagar","LB Nagar","Uppal","Miyapur","Bachupally","Kompally","Tolichowki","Mehdipatnam","Attapur","Chandanagar","Manikonda","Nanakramguda"],
    Chennai: ["Anna Nagar East","Anna Nagar West","T Nagar","Adyar","Velachery","Tambaram","Porur","Chromepet","Perambur","Ambattur","Avadi","Sholinganallur","OMR Perungudi","OMR Thoraipakkam","Nungambakkam","Egmore","Mylapore","Thiruvanmiyur","Pallavaram","Guindy","Kodambakkam","Mogappair","Kolathur","Madhavaram","Tondiarpet","Medavakkam","Pallikaranai","Navalur","Siruseri"],
    Pune: ["Koregaon Park","Kalyani Nagar","Viman Nagar","Kharadi","Wakad","Hinjewadi Phase 1","Hinjewadi Phase 2","Baner","Balewadi","Aundh","Kothrud","Deccan","FC Road","JM Road","Shivajinagar","Camp","Hadapsar","Kondhwa","Undri","Wanowrie","Magarpatta","Sinhagad Road","Katraj","Pimpri","Chinchwad","Akurdi","Nigdi","Bhosari","Wagholi","Pashan","Mundhwa"],
    Ahmedabad: ["Navrangpura","Satellite","Vastrapur","Bodakdev","Prahlad Nagar","SG Highway","Maninagar","Naroda","Chandkheda","Gota","Bopal","South Bopal","Thaltej","Vejalpur","Nikol","Vastral","Naranpura","Paldi","Ellis Bridge","CG Road","Gurukul","Memnagar","Ghatlodia"],
    Kolkata: ["Park Street","Salt Lake Sector 1","Salt Lake Sector 2","Salt Lake Sector 3","Salt Lake Sector 5","New Town Action Area 1","New Town Action Area 2","Rajarhat","Howrah","Dum Dum","Barasat","Behala","Tollygunge","Ballygunge","Alipore","Gariahat","Jadavpur","Sonarpur","Garia","Kasba","Shyambazar","Ultadanga","Lake Town","Baguiati","Kestopur","Madhyamgram","Barrackpore"],
    Jaipur: ["Malviya Nagar","Vaishali Nagar","Mansarovar","Raja Park","Civil Lines","MI Road","Tonk Road","Ajmer Road","Sikar Road","Jagatpura","Pratap Nagar","Sanganer","Murlipura","Nirman Nagar","C Scheme","Shyam Nagar","Sodala","Vidhyadhar Nagar","Durgapura","Sitapura"],
    Lucknow: ["Hazratganj","Gomti Nagar","Gomti Nagar Extension","Aliganj","Indira Nagar","Alambagh","Charbagh","Mahanagar","Jankipuram","Chinhat","Faizabad Road","Kanpur Road","Sitapur Road","Sushant Golf City","Vrindavan Yojana","Shaheed Path"],
    Surat: ["Adajan","Vesu","Athwa","Citylight","Pal","Althan","Bhatar","Katargam","Varachha","Udhna","Piplod","Dumas Road","Rander","Limbayat","Kapodra","Utran","Sachin","Amroli","Punagam"],
    Nagpur: ["Dharampeth","Sitabuldi","Sadar","Ramdaspeth","Wardha Road","Hingna","Katol Road","Kamptee Road","Mankapur","Nandanvan","Pratap Nagar","Abhyankar Nagar","Laxmi Nagar","Trimurti Nagar"],
    Kochi: ["Ernakulam South","Ernakulam North","MG Road Kochi","Edapally","Kakkanad","Aluva","Vyttila","Palarivattom","Kaloor","Thripunithura","Fort Kochi","Mattancherry","Cheranalloor"],
    Chandigarh: ["Sector 17","Sector 22","Sector 35","Sector 43","Sector 8","Sector 11","Mohali Phase 7","Mohali Phase 10","Mohali Phase 11","Panchkula Sector 10","Panchkula Sector 20","Zirakpur","Kharar","Derabassi"],
    "Coastal Karnataka": ["Bhatkal","Mangalore City Centre","Mangalore Bejai","Mangalore Hampankatta","Mangalore Kadri","Mangalore Kankanady","Udupi","Manipal","Kundapur","Karwar","Sirsi","Kumta","Honavar","Ankola","Bantwal","Puttur","Belthangady","Dharmasthala","Moodabidri","Sullia"],
    Indore: ["Vijay Nagar","Scheme 54","LIG Colony","AB Road","Palasia","Bhanwarkuan","Banganga","Rau","Rajwada","Sapna Sangeeta","Old Palasia","New Palasia"],
    Bhopal: ["MP Nagar Zone 1","MP Nagar Zone 2","Arera Colony","Kolar Road","Shahpura","Hoshangabad Road","Ayodhya Nagar","Govindpura","Bittan Market"],
    Coimbatore: ["RS Puram","Gandhipuram","Peelamedu","Saibaba Colony","Singanallur","Avinashi Road","Mettupalayam Road","Podanur","Thudiyalur"],
    Visakhapatnam: ["MVP Colony","Dwaraka Nagar","Seethammadhara","Steel Plant Area","Gajuwaka","Madhurawada","Rushikonda","Kommadi"],
    Patna: ["Boring Road","Bailey Road","Kankarbagh","Rajendra Nagar","Danapur","Phulwarisharif","Patliputra Colony"],
  },
  Indonesia: {
    Jakarta: ["Sudirman","Kuningan","Kemang","Menteng","Kelapa Gading","PIK","Pluit","Grogol"],
    Bali: ["Seminyak","Kuta","Ubud","Canggu","Sanur","Jimbaran","Nusa Dua"],
    Surabaya: ["Surabaya City Centre","Gubeng","Wiyung","Rungkut"],
  },
  Ireland: {
    Dublin: ["Dublin City Centre","Dublin 2 Southside","Dublin 4 Ballsbridge","Dublin 6 Rathmines","Dublin 7 Phibsborough","Dublin 8 Portobello","Dublin 12 Crumlin","Dublin 15 Blanchardstown","Dun Laoghaire","Swords","Tallaght"],
    Cork: ["Cork City Centre","Cork Douglas","Cork Ballincollig"],
    Galway: ["Galway City Centre","Salthill"],
    Limerick: ["Limerick City Centre"],
    Waterford: ["Waterford City Centre"],
  },
  Israel: {
    "Tel Aviv": ["Tel Aviv City Centre","Florentin","Neve Tzedek","Ramat Aviv","Herzliya","Ramat Gan"],
    Jerusalem: ["Jerusalem City Centre","Malha","Talpiot"],
    Haifa: ["Haifa City Centre","Hadar","Carmel"],
  },
  Italy: {
    Rome: ["Rome Prati","Rome Trastevere","Rome EUR","Rome Parioli","Rome Ostia","Rome Centocelle"],
    Milan: ["Milan Navigli","Milan Porta Venezia","Milan Isola","Milan Brera","Milan Porta Garibaldi","Milan Moscova","Milan CityLife"],
    Naples: ["Naples Chiaia","Naples Vomero","Naples Fuorigrotta","Naples Posillipo"],
    Turin: ["Turin City Centre","Turin Crocetta","Turin Lingotto"],
    Florence: ["Florence City Centre","Florence Oltrarno","Florence Settignano"],
    Bologna: ["Bologna City Centre","Bologna Mazzini"],
    Venice: ["Venice Mestre","Venice Marghera","Venice Lido"],
    Bari: ["Bari City Centre","Bari Poggiofranco"],
    Catania: ["Catania City Centre","Catania Librino"],
    Palermo: ["Palermo City Centre","Palermo Mondello"],
  },
  Japan: {
    Tokyo: ["Shinjuku","Shibuya","Roppongi","Akihabara","Ginza","Harajuku","Asakusa","Ikebukuro","Shinagawa"],
    Osaka: ["Namba","Shinsaibashi","Umeda","Tennoji","Abeno"],
    Kyoto: ["Kyoto City Centre","Gion","Fushimi"],
    Yokohama: ["Yokohama Minato Mirai","Kannai","Isezakicho"],
  },
  Jordan: {
    Amman: ["Abdoun","Sweifieh","Shmeisani","Jabal Amman","Tlaa Al Ali","Mecca Street Amman","Dabouq"],
    Aqaba: ["Aqaba City","Aqaba South Beach"],
  },
  Kenya: {
    Nairobi: ["Westlands","Karen","Lavington","Kilimani","Upperhill","Parklands","South B","South C"],
    Mombasa: ["Mombasa City","Nyali","Bamburi","Mtwapa"],
  },
  Kuwait: {
    "Kuwait City": ["Sharq","Qibla","Salmiya","Hawalli","Rumaithiya","Mishref","Bayan","Salwa","Farwaniya","Abu Halifa","Mangaf","Fintas","Mahboula","Fahaheel","Ahmadi","Jahra","Bneid Al Qar"],
  },
  Lebanon: {
    Beirut: ["Hamra","Achrafieh","Verdun","Raouche","Badaro","Gemmayzeh","Mar Mikhael"],
    "Mount Lebanon": ["Jounieh","Dbayeh","Zouk Mosbeh","Baabda","Aley"],
  },
  Malaysia: {
    "Kuala Lumpur": ["KLCC","Bukit Bintang","Chow Kit","Bangsar","Mont Kiara","Sri Hartamas","Ampang","Pandan Jaya"],
    "Petaling Jaya": ["PJ SS2","PJ Damansara","Subang Jaya SS15","Shah Alam Seksyen 7","Puchong","Klang","Ara Damansara"],
    "Johor Bahru": ["JB City Square","JB Tebrau","Iskandar Puteri","Skudai"],
    Penang: ["Georgetown","Bayan Lepas","Butterworth","Batu Ferringhi"],
    "Other Malaysia": ["Ipoh","Kota Kinabalu","Kuching Sarawak","Kuantan","Melaka City","Seremban"],
  },
  Mexico: {
    "Mexico City": ["Polanco","Roma Norte","Condesa","Santa Fe","Coyoacán","Tlalpan","Tepito"],
    Guadalajara: ["Chapalita","Providencia","Tlaquepaque","Zapopan"],
    Monterrey: ["San Pedro Garza García","Cumbres","Mitras","Contry"],
  },
  Morocco: {
    Casablanca: ["Maarif","Anfa","Ain Diab","Hay Hassani","Belvédère"],
    Rabat: ["Agdal","Hassan","Souissi","Hay Riad"],
    Marrakech: ["Gueliz","Hivernage","Palmeraie","Medina Marrakech"],
  },
  Netherlands: {
    Amsterdam: ["Amsterdam Centrum","Amsterdam Oost","Amsterdam Zuidoost","Amsterdam Noord","Amsterdam Nieuw-West","Amsterdam Zuidas","Amsterdam De Pijp"],
    Rotterdam: ["Rotterdam Centrum","Rotterdam Feijenoord","Rotterdam Kralingen","Rotterdam Delfshaven"],
    "The Hague": ["The Hague Centrum","The Hague Laak","The Hague Scheveningen","The Hague Loosduinen"],
    Utrecht: ["Utrecht Centrum","Utrecht Overvecht","Utrecht Leidsche Rijn"],
    Eindhoven: ["Eindhoven City Centre","Eindhoven Strijp"],
    Tilburg: ["Tilburg City Centre"],
    Groningen: ["Groningen City Centre"],
    Almere: ["Almere City Centre","Almere Buiten"],
    Breda: ["Breda City Centre"],
    Arnhem: ["Arnhem City Centre"],
  },
  "New Zealand": {
    Auckland: ["Auckland CBD","Manukau","North Shore","Waitakere","Newmarket","Takapuna","Henderson","Papakura","Pukekohe","Botany Downs","Pakuranga"],
    Wellington: ["Wellington CBD","Lower Hutt","Upper Hutt","Porirua","Karori","Island Bay"],
    Christchurch: ["Christchurch CBD","Riccarton","Linwood","Spreydon","Burnside"],
    "Other NZ": ["Hamilton","Tauranga","Rotorua","Dunedin","Palmerston North","Napier","Hastings","Nelson","Whangarei","New Plymouth","Invercargill"],
  },
  Nigeria: {
    Lagos: ["Victoria Island","Lekki Phase 1","Ikoyi","Surulere","Ikeja","Yaba","Ajah","Sangotedo"],
    Abuja: ["Wuse 2","Maitama","Asokoro","Garki","Gwarinpa"],
    Kano: ["Kano City Centre","Sabon Gari","Fagge"],
  },
  Norway: {
    Oslo: ["Oslo City Centre","Oslo Grünerløkka","Oslo Frogner","Oslo Majorstuen","Oslo Bjørvika","Oslo St Hanshaugen"],
    Bergen: ["Bergen City Centre","Bergenhus","Fana"],
    Trondheim: ["Trondheim City Centre","Trondheim Singsaker"],
    Stavanger: ["Stavanger City Centre","Stavanger Madla"],
  },
  Oman: {
    Muscat: ["Qurum","Al Khuwair","Madinat Sultan Qaboos","Shatti Al Qurum","Seeb","Bausher","Muttrah","Ruwi","Ghubra","Al Mouj","Azaiba","Maabelah","Al Hail","Bowshar","Al Ansab","Wadi Kabir","Al Wattayah","Darsait"],
    Salalah: ["Salalah City Centre","Al Haffa Salalah","Raysut","Itin","Dahariz","Taqah","Mirbat","Sadah"],
    Sohar: ["Sohar City","Sohar Industrial","Falaj Al Qabail","Al Multaqa"],
    "Sur": ["Sur City","Sur Industrial","Ras Al Hadd"],
    Nizwa: ["Nizwa City","Nizwa Souk Area","Birkat Al Mouz"],
    Ibri: ["Ibri City","Ibri Souk"],
    Buraimi: ["Buraimi City","Al Mahdha"],
    Khasab: ["Khasab City","Musandam"],
    "Al Buraymi": ["Buraymi City Centre"],
    "Rustaq": ["Rustaq City","Awabi"],
    Duqm: ["Duqm Special Economic Zone"],
    "Al Amerat": ["Al Amerat City","Al Khoudh"],
  },
  Pakistan: {
    Karachi: ["DHA Karachi","Clifton","Gulshan-e-Iqbal","PECHS","Saddar","Malir","Korangi"],
    Lahore: ["DHA Lahore","Gulberg","Model Town","Bahria Town Lahore","Johar Town","Cantt"],
    Islamabad: ["F-6 Islamabad","F-7 Islamabad","F-10 Islamabad","G-9 Islamabad","DHA Islamabad","Bahria Town Islamabad"],
  },
  Philippines: {
    Manila: ["Makati","BGC Taguig","Ortigas","Quezon City","Pasig","Mandaluyong","Paranaque"],
    Cebu: ["Cebu City","Lapu-Lapu","Mandaue"],
    Davao: ["Davao City Centre","Buhangin","Toril"],
  },
  Poland: {
    Warsaw: ["Warsaw Srodmiescie","Warsaw Mokotow","Warsaw Ursynow","Warsaw Praga Polnoc","Warsaw Praga Poludnie","Warsaw Wola","Warsaw Ochota","Warsaw Wilanow","Warsaw Bielany","Warsaw Zoliborz"],
    Krakow: ["Krakow Stare Miasto","Krakow Nowa Huta","Krakow Podgorze","Krakow Krowodrza","Krakow Bronowice"],
    Wroclaw: ["Wroclaw Srodmiescie","Wroclaw Krzyki","Wroclaw Fabryczna"],
    Poznan: ["Poznan Stare Miasto","Poznan Grunwald","Poznan Nowe Miasto"],
    Gdansk: ["Gdansk City Centre","Gdansk Wrzeszcz","Sopot","Gdynia"],
    Lodz: ["Lodz City Centre","Lodz Polesie"],
    Katowice: ["Katowice City Centre","Tychy","Sosnowiec"],
  },
  Portugal: {
    Lisbon: ["Lisbon Chiado","Lisbon Bairro Alto","Lisbon Alfama","Lisbon Parque das Nações","Lisbon Belem","Lisbon Cascais","Lisbon Almada","Lisbon Sintra"],
    Porto: ["Porto City Centre","Porto Matosinhos","Porto Gaia","Porto Boavista","Porto Foz do Douro"],
    "Other Portugal": ["Braga","Faro","Algarve Portimao","Algarve Lagos","Algarve Albufeira","Coimbra","Setúbal"],
  },
  Qatar: {
    Doha: ["West Bay","The Pearl","Lusail Marina","Lusail Fox Hills","Al Sadd","Al Rayyan","Madinat Khalifa North","Madinat Khalifa South","Msheireb Downtown","Al Dafna","Old Airport Road","Ain Khaled","Al Gharrafa","Al Aziziya","Muaither","Al Hilal","Al Mansoura","Fereej Bin Omran"],
  },
  Romania: {
    Bucharest: ["Floreasca","Dorobanți","Cotroceni","Titan","Drumul Taberei","Berceni"],
    Cluj: ["Cluj City Centre","Mărăști","Gheorgheni"],
    Timișoara: ["Timișoara City Centre","Fabric"],
  },
  "Saudi Arabia": {
    Riyadh: ["Olaya District","Al Malaz","Al Nakheel","King Fahd District","Hittin","Al Sahafah","Diplomatic Quarter","Al Yasmin","Al Ghadir","Al Wurud","Al Malqa","Al Narjis","Al Qirawan","Al Maather","Ash Shuhada","Al Falah","Al Badeah","Al Munsiyah","Al Rawdah Riyadh"],
    Jeddah: ["Al Balad","Al Andalus","Al Rawdah Jeddah","Al Shati","Al Hamra Jeddah","Obhur Al Shamaliyah","Al Safa","Al Zahraa","Al Khalidiyah Jeddah","Al Muhammadiyah","Al Azizia Jeddah","Al Rehab Jeddah","Al Nuzha Jeddah","Al Bawadi Jeddah","Al Worood Jeddah"],
    Dammam: ["Al Khobar Corniche","Al Khobar Thuqbah","Al Khobar Rakah","Dhahran","Qatif","Jubail","Ras Tanura","Abqaiq","Al Hamra Dammam"],
    Mecca: ["Al Aziziah Mecca","Ajyad","Al Masfalah","Kudai","Jarwal","Ash Shara","Al Adl Mecca"],
    Medina: ["Al Noor Medina","Al Aziziah Medina","Quba","Al Iskan Medina","Al Rawabi Medina"],
    Taif: ["Taif City Centre","Al Hawiyah","Al Shafa","Al Hada","Al Wasl Taif"],
    Abha: ["Abha City Centre","Al Numas","Khamis Mushait"],
    Tabuk: ["Tabuk City Centre","Al Rawdah Tabuk"],
    Hail: ["Hail City Centre"],
  },
  Singapore: {
    Singapore: ["Orchard Road","Marina Bay","Bugis","Tampines","Jurong East","Woodlands","Ang Mo Kio","Bishan","Toa Payoh","Clementi","Bedok","Hougang","Sengkang","Yishun","Serangoon","Buona Vista","Queenstown","Punggol","Pasir Ris","Jurong West","Bukit Timah","Novena"],
  },
  "South Africa": {
    Johannesburg: ["Sandton","Rosebank","Melville","Soweto","Randburg","Edenvale","Roodepoort"],
    "Cape Town": ["Waterfront","Green Point","Sea Point","Camps Bay","Stellenbosch","Paarl","Somerset West"],
    Durban: ["Umhlanga","Ballito","Berea Durban","Morningside Durban"],
  },
  "South Korea": {
    Seoul: ["Gangnam","Hongdae","Itaewon","Insadong","Sinchon","Mapo","Jongno"],
    Busan: ["Haeundae","Seomyeon","Nampo"],
    Incheon: ["Incheon City Centre","Songdo"],
  },
  Spain: {
    Madrid: ["Madrid Salamanca","Madrid Chamberí","Madrid Retiro","Madrid Tetuan","Madrid Vallecas","Madrid Carabanchel","Madrid Pozuelo","Madrid Alcobendas","Madrid Boadilla","Madrid Las Rozas"],
    Barcelona: ["Barcelona Eixample Dreta","Barcelona Eixample Esquerra","Barcelona Gràcia","Barcelona Sants","Barcelona Poblenou","Barcelona Sarria","Barcelona Nou Barris","Barcelona Sant Andreu"],
    Valencia: ["Valencia City Centre","Valencia Ruzafa","Valencia Benimaclet","Valencia El Cabanyal"],
    Seville: ["Seville City Centre","Seville Triana","Seville Nervión"],
    Bilbao: ["Bilbao City Centre","Bilbao Deusto","Barakaldo"],
    Malaga: ["Malaga City Centre","Malaga Fuengirola","Malaga Torremolinos","Malaga Marbella","Malaga Estepona"],
    Alicante: ["Alicante City Centre","Benidorm","Torrevieja"],
    "Zaragoza": ["Zaragoza City Centre","Zaragoza Delicias"],
    "Palma Mallorca": ["Palma City Centre","Palma Son Armadans"],
  },
  Sweden: {
    Stockholm: ["Stockholm Norrmalm","Stockholm Södermalm","Stockholm Vasastan","Stockholm Östermalm","Stockholm Kungsholmen","Stockholm Nacka","Stockholm Solna","Stockholm Sundbyberg"],
    Gothenburg: ["Gothenburg City Centre","Gothenburg Hisingen","Gothenburg Mölndal"],
    Malmö: ["Malmö City Centre","Malmö Hyllie","Malmö Husie"],
    Uppsala: ["Uppsala City Centre"],
    Linköping: ["Linköping City Centre"],
  },
  Switzerland: {
    Zurich: ["Zurich City Centre","Zurich Oerlikon","Zurich Wiedikon","Zurich Altstetten","Zurich Wollishofen","Zurich Höngg","Zurich Affoltern"],
    Geneva: ["Geneva City Centre","Geneva Carouge","Geneva Meyrin","Geneva Lancy","Geneva Vernier"],
    Basel: ["Basel City Centre","Basel Kleinbasel","Basel Allschwil"],
    Bern: ["Bern City Centre","Bern Bethlehem","Bern Bümpliz"],
    Lausanne: ["Lausanne City Centre","Lausanne Prilly"],
    Winterthur: ["Winterthur City Centre"],
    Lucerne: ["Lucerne City Centre"],
    "St Gallen": ["St Gallen City Centre"],
    Lugano: ["Lugano City Centre"],
  },
  Thailand: {
    Bangkok: ["Sukhumvit","Silom","Siam","Ari","Thonglor","Ekkamai","Ratchada","Ladprao"],
    "Chiang Mai": ["Chiang Mai Old City","Nimmanhaemin","Santitham"],
    Phuket: ["Patong","Kata","Karon","Rawai","Chalong"],
  },
  Turkey: {
    Istanbul: ["Besiktas","Beyoglu","Sisli","Kadikoy","Atasehir","Bakirkoy","Fatih","Uskudar"],
    Ankara: ["Cankaya","Kizilay","Bahcelievler","Etimesgut"],
    Izmir: ["Konak","Karsiyaka","Bornova","Buca"],
  },
  UAE: {
    Dubai: ["Dubai Marina","JBR","Palm Jumeirah","Downtown Dubai","Business Bay","DIFC","Jumeirah 1","Jumeirah 2","Jumeirah 3","JLT Cluster A","JLT Cluster T","JVC","JVT","Al Barsha 1","Al Barsha 2","Al Barsha 3","Deira Naif","Deira Rigga","Deira Al Qusais","Deira Al Nahda","Bur Dubai Mankhool","Bur Dubai Satwa","Karama","Oud Metha","Al Quoz Residential","Al Quoz Industrial","Mirdif","Dubai Hills Estate","Silicon Oasis","International City","Arabian Ranches","Motor City","Al Furjan","Dubai South","Mohammed Bin Rashid City","Bluewaters Island"],
    "Abu Dhabi": ["Khalidiyah","Corniche Road","Hamdan Street","Electra Street","Al Reem Island","Al Maryah Island","Yas Island","Saadiyat Island","Khalifa City A","Khalifa City B","Mohammed Bin Zayed City","Mussafah Residential","Mussafah Industrial","Baniyas","Al Ain City","Al Ain Al Jimi","Al Ain Zakher","Al Ain Hili","Al Ain Muwaiji","Airport Road Abu Dhabi","Al Zahiyah","Al Bateen","Al Mushrif","Al Muroor"],
    Sharjah: ["Sharjah City Centre","Al Nahda Sharjah","Al Majaz 1","Al Majaz 2","Al Majaz 3","Al Taawun","Muwaileh","Al Khan","Abu Shagara","Al Qasimia","Al Yarmook","Al Gharb","Al Khalidiya Sharjah"],
    Ajman: ["Ajman City Centre","Al Rashidiya Ajman","Al Nuaimia 1","Al Nuaimia 2","Al Jurf","Emirates City Ajman","Al Rawda Ajman","Mushairif Ajman"],
    "Ras Al Khaimah": ["RAK City","Al Nakheel RAK","Al Hamra Village","Mina Al Arab","Al Dhait","Khuzam","Al Mairid","Al Mamourah"],
    Fujairah: ["Fujairah City","Dibba Al Fujairah","Khor Fakkan","Kalba","Al Faseel"],
    "Umm Al Quwain": ["UAQ City","Al Raas UAQ","Al Salama UAQ","Al Raudah UAQ"],
  },
  Uganda: {
    Kampala: ["Kololo","Nakasero","Bugolobi","Ntinda","Muyenga","Makindye"],
  },
  Ukraine: {
    Kyiv: ["Pechersk","Podil","Shevchenkivskyi","Obolon","Darnytsia"],
    Lviv: ["Lviv City Centre","Shevchenkivskyi Lviv"],
    Odessa: ["Odessa City Centre","Arkadia"],
  },
  "United Kingdom": {
    London: ["Westminster","Soho","Covent Garden","Shoreditch","Canary Wharf","Brixton","Hackney","Islington","Camden","Hammersmith","Ealing","Croydon","Stratford","Greenwich","Lewisham","Southwark","Lambeth","Tower Hamlets","Newham","Barking","Enfield","Walthamstow","Wimbledon","Fulham","Chelsea","Kensington","Paddington","Elephant Castle","Peckham","Tottenham","Wood Green","Finchley","Harrow","Wembley","Uxbridge","Romford","Ilford","Clapham","Balham","Tooting","Streatham","Norwood","Sutton","Kingston","Richmond","Twickenham","Hounslow","Acton"],
    Manchester: ["City Centre","Salford","Didsbury","Withington","Chorlton","Stretford","Trafford","Stockport","Oldham","Bolton","Bury","Rochdale","Ashton under Lyne","Sale","Altrincham","Wigan","Leigh"],
    Birmingham: ["City Centre","Edgbaston","Solihull","Erdington","Selly Oak","Kings Heath","Handsworth","Sparkhill","Ladywood","Harborne","Moseley","Hall Green","Wolverhampton","West Bromwich","Sutton Coldfield","Dudley","Walsall"],
    Leeds: ["City Centre","Headingley","Chapel Allerton","Roundhay","Morley","Pudsey","Horsforth","Garforth","Beeston"],
    Glasgow: ["City Centre","West End","East End","Southside","Govan","Partick","Shawlands","Rutherglen","Paisley","Motherwell"],
    Edinburgh: ["Old Town","New Town","Leith","Morningside","Bruntsfield","Newington","Stockbridge","Portobello","Corstorphine"],
    Liverpool: ["City Centre","Wavertree","Toxteth","Aigburth","Woolton","Birkenhead","Wallasey","Bootle"],
    Bristol: ["City Centre","Clifton","Bedminster","Southville","Brislington","Horfield","Filton"],
    Sheffield: ["City Centre","Ecclesall Road","Hillsborough","Rotherham"],
    Newcastle: ["City Centre","Gateshead","Sunderland","Wallsend","Whitley Bay"],
    Nottingham: ["City Centre","West Bridgford","Arnold","Long Eaton"],
    Leicester: ["City Centre","Oadby","Loughborough"],
    Cardiff: ["Cardiff City Centre","Cardiff Bay","Roath","Canton"],
    Oxford: ["Oxford City Centre","Cowley","Headington"],
    Cambridge: ["Cambridge City Centre","Chesterton","Trumpington"],
    Brighton: ["Brighton City Centre","Hove","Kemp Town"],
  },
  USA: {
    "New York": ["Midtown Manhattan","Lower Manhattan","Upper East Side","Upper West Side","Harlem","Washington Heights","Williamsburg Brooklyn","Park Slope Brooklyn","Bay Ridge Brooklyn","Flatbush Brooklyn","Crown Heights Brooklyn","Bushwick Brooklyn","Astoria Queens","Flushing Queens","Jamaica Queens","Forest Hills Queens","Long Island City","Bronx Fordham","Bronx Riverdale","Staten Island","Jersey City","Hoboken","Newark","Long Island Garden City","White Plains","Yonkers","Stamford CT"],
    "Los Angeles": ["Downtown","Santa Monica","Beverly Hills","Hollywood","West Hollywood","Koreatown","Silver Lake","Echo Park","Culver City","Venice Beach","Pasadena","Burbank","Glendale","Torrance","Inglewood","Long Beach","Anaheim","Irvine","Costa Mesa","Santa Ana","Fullerton","Pomona","Ontario","Rancho Cucamonga"],
    Chicago: ["Downtown Loop","Lincoln Park","Wicker Park","Hyde Park","Pilsen","Bridgeport","Rogers Park","Andersonville","Evanston","Oak Park","Schaumburg","Naperville","Aurora","Joliet","Waukegan","Elgin","Cicero","Berwyn","Oak Lawn"],
    Houston: ["Downtown","Midtown","Galleria","Montrose","Heights","Westheimer","Sugar Land","Katy","Pearland","The Woodlands","Pasadena TX","Baytown","League City","Missouri City","Spring","Humble"],
    Dallas: ["Downtown","Uptown","Deep Ellum","Oak Cliff","Plano","Frisco","McKinney","Allen","Arlington","Fort Worth Downtown","Fort Worth Southside","Irving","Garland","Mesquite","Denton","Carrollton","Richardson","Lewisville","Flower Mound","Grapevine"],
    Miami: ["Downtown","Miami Beach South Beach","Miami Beach North Beach","Brickell","Wynwood","Coral Gables","Little Havana","Hialeah","Doral","Kendall","Aventura","Pompano Beach","Fort Lauderdale Downtown","Fort Lauderdale Beach","Hollywood FL","Miramar","Pembroke Pines"],
    "San Francisco": ["Downtown","Mission District","Castro","Richmond District","Sunset District","SoMa","Oakland Downtown","Oakland Temescal","Berkeley","San Jose Downtown","Sunnyvale","Santa Clara","Palo Alto","Mountain View","Redwood City","Daly City","San Mateo"],
    Phoenix: ["Downtown Phoenix","Scottsdale Old Town","Scottsdale North","Tempe","Mesa","Chandler","Gilbert","Glendale","Peoria","Surprise","Goodyear"],
    Philadelphia: ["Downtown","South Philly","North Philly","West Philly","Northeast Philadelphia","Manayunk","Camden NJ","Cherry Hill NJ"],
    Seattle: ["Downtown","Capitol Hill","Fremont","Ballard","Bellevue","Redmond","Kirkland","Tacoma","Renton","Lynnwood","Everett"],
    Boston: ["Downtown","Back Bay","Fenway","South Boston","Jamaica Plain","Cambridge Harvard Square","Cambridge MIT","Somerville","Brookline","Quincy","Medford"],
    Atlanta: ["Downtown","Midtown","Buckhead","Decatur","Sandy Springs","Marietta","Smyrna","Roswell","Alpharetta","Johns Creek"],
    "Las Vegas": ["Las Vegas Strip","Downtown Las Vegas","Henderson","North Las Vegas","Summerlin","Henderson Green Valley"],
    Denver: ["Downtown","Capitol Hill","Cherry Creek","Aurora","Lakewood","Arvada","Westminster","Englewood","Thornton"],
    "San Diego": ["Downtown","Mission Valley","El Cajon","Chula Vista","Escondido","Oceanside","Carlsbad","Encinitas","La Jolla"],
    "San Antonio": ["Downtown","Alamo Heights","Stone Oak","Northside","Medical Center"],
    Portland: ["Downtown Portland OR","Pearl District","Southeast Portland","Northeast Portland","Beaverton","Hillsboro","Gresham"],
    Minneapolis: ["Downtown Minneapolis","Uptown Minneapolis","Bloomington MN","St Paul","Edina"],
    Tampa: ["Downtown Tampa","Ybor City","St Petersburg FL","Clearwater FL"],
    Orlando: ["Downtown Orlando","International Drive","Winter Park FL","Kissimmee"],
    Charlotte: ["Downtown Charlotte","South End","Ballantyne","Concord NC"],
    Detroit: ["Downtown Detroit","Midtown Detroit","Royal Oak MI","Ann Arbor MI"],
    Nashville: ["Downtown Nashville","The Gulch","Brentwood TN","Franklin TN"],
    Baltimore: ["Downtown Baltimore","Inner Harbor","Towson MD"],
    "Kansas City": ["Downtown KC","Plaza KC","Overland Park KS"],
    Columbus: ["Downtown Columbus","Short North","Dublin OH"],
    Indianapolis: ["Downtown Indy","Broad Ripple"],
    "Salt Lake City": ["Downtown SLC","Sugar House","Sandy UT"],
    "New Orleans": ["French Quarter","Garden District","Uptown New Orleans","Metairie"],
    Raleigh: ["Downtown Raleigh","North Hills","Cary NC","Durham NC"],
    Pittsburgh: ["Downtown Pittsburgh","Shadyside","South Side"],
  },
  Vietnam: {
    "Ho Chi Minh City": ["District 1","District 2","District 3","Binh Thanh","Phu Nhuan","Go Vap"],
    Hanoi: ["Hoan Kiem","Ba Dinh","Dong Da","Tay Ho","Cau Giay"],
    "Da Nang": ["Da Nang City","My Khe Beach","Hoi An"],
  },
}

const PROFESSIONS = ["dentist","dental clinic","gym","fitness center","yoga studio","lawyer","ca firm","real estate agent","restaurant","cafe","hotel","hospital","clinic","pharmacy","salon","spa","school","coaching center","plumber","electrician","car service","jeweller","optician","accountant","architect","interior designer","photography studio","bakery","supermarket","bank","insurance agent"]

export default function DashboardPage({ user, onLogout }) {
  const navigate = useNavigate()
  const ACTIVE_JOB_KEY = `ls_active_job_${user.id}`
  const [countrySearch, setCountrySearch] = useState("")
  const [country, setCountry]   = useState("")
  const [city, setCity]         = useState("")
  const [areas, setAreas]       = useState([])
  const [selAreas, setSelAreas] = useState([])
  const [newArea, setNewArea]   = useState("")
  const [profession, setProfession] = useState("dentist")
  const [customPro, setCustomPro]   = useState("")
  const [scraping, setScraping]     = useState(false)
  const [leads, setLeads]           = useState([])
  const [progress, setProgress]     = useState({ current: 0, total: 0, query: "" })
  const [stats, setStats]           = useState({ total: 0, withOwner: 0, withEmail: 0, withWebsite: 0 })
  const [jobId, setJobId]           = useState(null)
  const [canDownload, setCanDownload] = useState(false)
  const [resumeCandidate, setResumeCandidate] = useState(null)
  const [resumeBusy, setResumeBusy] = useState(false)
  const [showCountryDD, setShowCountryDD] = useState(false)
  const [liveUrl, setLiveUrl] = useState({ maps_url: "", website_url: "", name: "" })
  const [history, setHistory] = useState([])
  const [showHistory, setShowHistory] = useState(false)
  const [runtimeStatus, setRuntimeStatus] = useState("")
  const [runtimeErrorDetails, setRuntimeErrorDetails] = useState(null)

  // ── v2 multi-platform config ──────────────────────────────
  const [queries, setQueries]           = useState(["dentist"])
  const [queryInput, setQueryInput]     = useState("")
  const [enableMaps, setEnableMaps]     = useState(true)
  const [enableJD, setEnableJD]         = useState(true)
  const [enableIM, setEnableIM]         = useState(true)
  const [websiteFilter, setWebsiteFilter] = useState("minimal")
  const [maxPerQuery, setMaxPerQuery]   = useState(25)
  const [useProxy, setUseProxy]         = useState(false)
  const [proxyStats, setProxyStats]     = useState(null)   // { live, fastest_ms, by_protocol }
  const [customCity, setCustomCity]     = useState("")
  const [usage, setUsage]               = useState(null)
  const wsRef    = useRef(null)
  const keepSocketAliveRef = useRef(true)
  const tableRef = useRef(null)
  const countryRef = useRef(null)
  const pendingLeadsRef = useRef([])
  const pendingStatsRef = useRef({ total: 0, withOwner: 0, withEmail: 0, withWebsite: 0 })
  const flushTimerRef = useRef(null)
  const heartbeatRef = useRef(null)
  const retryCountRef = useRef(0)
  const seenLeadKeysRef = useRef(new Set())

  const resetLiveBuffers = () => {
    pendingLeadsRef.current = []
    pendingStatsRef.current = { total: 0, withOwner: 0, withEmail: 0, withWebsite: 0 }
    seenLeadKeysRef.current = new Set()
    if (flushTimerRef.current) {
      clearTimeout(flushTimerRef.current)
      flushTimerRef.current = null
    }
  }

  const authRequest = (url, options = {}) =>
    authFetch(url, options, () => navigate("/login"))

  const flushLiveFeed = () => {
    const leadsBatch = pendingLeadsRef.current
    const delta = pendingStatsRef.current
    pendingLeadsRef.current = []
    pendingStatsRef.current = { total: 0, withOwner: 0, withEmail: 0, withWebsite: 0 }
    flushTimerRef.current = null

    if (leadsBatch.length > 0) {
      const newestFirst = [...leadsBatch].reverse()
      setLeads(prev => [...newestFirst, ...prev].slice(0, LIVE_FEED_MAX_ROWS))
    }

    if (delta.total > 0 || delta.withOwner > 0 || delta.withEmail > 0 || delta.withWebsite > 0) {
      setStats(prev => ({
        total: prev.total + delta.total,
        withOwner: prev.withOwner + delta.withOwner,
        withEmail: prev.withEmail + delta.withEmail,
        withWebsite: prev.withWebsite + delta.withWebsite,
      }))
    }
  }

  const makeLeadKey = (lead) => {
    const phone = (lead.Phone || "").replace(/[^0-9]/g, "").trim()
    if (phone.length >= 6) return `phone::${phone}`
    const name = (lead.Name || "").toLowerCase().trim()
    const addr = (lead.Address || "").toLowerCase().trim()
    return `name::${name}::addr::${addr}`
  }

  const queueLeadForUi = (lead) => {
    // DEDUP: Skip leads already shown in this session
    const key = makeLeadKey(lead)
    if (seenLeadKeysRef.current.has(key)) return
    seenLeadKeysRef.current.add(key)

    pendingLeadsRef.current.push(lead)
    pendingStatsRef.current.total += 1
    if (lead.Owner_Name) pendingStatsRef.current.withOwner += 1
    if (lead.Email || lead.Owner_Email_Guesses) pendingStatsRef.current.withEmail += 1
    if (lead.Website) pendingStatsRef.current.withWebsite += 1

    if (!flushTimerRef.current) {
      flushTimerRef.current = setTimeout(flushLiveFeed, LIVE_FEED_FLUSH_MS)
    }
  }

  const allCountries = useMemo(() => Object.keys(WORLD).sort(), [])
  const activePlan = (usage?.plan || user?.plan || "starter").toLowerCase()
  const planLimits = usage?.limits || null
  const isStarterPlan = activePlan === "starter"
  const filteredCountries = useMemo(() => {
    const q = countrySearch.toLowerCase()
    return allCountries.filter(c => c.toLowerCase().includes(q))
  }, [allCountries, countrySearch])
  const cities = useMemo(() => (country ? Object.keys(WORLD[country]).sort() : []), [country])

  useEffect(() => {
    if (!isStarterPlan) return
    setEnableMaps(true)
    setEnableJD(false)
    setEnableIM(false)
  }, [isStarterPlan])

  useEffect(() => {
    if (city && country) {
      const a = WORLD[country][city] || []
      setAreas(a); setSelAreas([...a])
    }
  }, [city, country])

  // Close dropdown on outside click
  useEffect(() => {
    const handle = (e) => {
      if (countryRef.current && !countryRef.current.contains(e.target))
        setShowCountryDD(false)
    }
    document.addEventListener("mousedown", handle)
    return () => document.removeEventListener("mousedown", handle)
  }, [])

  const toggle = (a) => setSelAreas(p => p.includes(a) ? p.filter(x => x !== a) : [...p, a])

  const buildNiche = () => {
    const finalPro = customPro || profession || "custom"
    return `${finalPro}_${city || country || "custom"}`.replace(/ /g, "_").toLowerCase()
  }

  const refreshResumeCandidate = async () => {
    try {
      const res = await authRequest(`/user/history`)
      if (!res.ok) throw new Error("history failed")
      const data = await res.json()
      const history = data.history || []

      // Only the most recent scrape is eligible for resume
      const mostRecent = history[0]
      if (!mostRecent || mostRecent.status !== "stopped") {
        setResumeCandidate(null)
        return
      }

      let total = Number(mostRecent.total_areas || 0)
      if (!total) {
        try { total = JSON.parse(mostRecent.areas || "[]").length } catch { total = 0 }
      }
      const processed = Number(mostRecent.processed_areas || 0)
      if (total <= processed) {
        setResumeCandidate(null)
        return
      }

      setResumeCandidate({
        jobId: mostRecent.job_id,
        remaining: Math.max(0, total - processed),
        processed,
        total,
        profession: mostRecent.profession,
      })
    } catch {
      setResumeCandidate(null)
    }
  }

  const refreshUsage = async () => {
    try {
      const res = await authRequest(`/user/usage`)
      if (!res.ok) throw new Error("usage failed")
      const data = await res.json()
      setUsage(data)
    } catch {
      setUsage(null)
    }
  }

  const clearActiveJob = () => {
    localStorage.removeItem(ACTIVE_JOB_KEY)
  }

  const canReconnectJob = async (id) => {
    try {
      const res = await authRequest(`/scrape/status/${id}`)
      if (!res.ok) return false
      const status = await res.json()
      return !!status?.running
    } catch {
      return false
    }
  }

  const resumeScrape = async (restartFromBeginning = false) => {
    if (!resumeCandidate?.jobId || resumeBusy) return
    setResumeBusy(true)
    keepSocketAliveRef.current = true
    resetLiveBuffers()
    setLeads([])
    setCanDownload(false)
    setRuntimeStatus("")
    setStats({ total: 0, withOwner: 0, withEmail: 0, withWebsite: 0 })
    setProgress({ current: 0, total: 0, query: restartFromBeginning ? "Restarting..." : "Resuming..." })
    setScraping(true)

    try {
      const res = await authRequest(`/scrape/resume/${resumeCandidate.jobId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: user.id,
          restart_from_beginning: restartFromBeginning,
          niche: restartFromBeginning ? buildNiche() : undefined,
        }),
      })
      if (!res.ok) throw new Error("resume failed")

      const data = await res.json()
      setJobId(data.job_id)
      localStorage.setItem(ACTIVE_JOB_KEY, data.job_id)
      attachSocket(data.job_id, { reset: true })
      await refreshResumeCandidate()
      await refreshUsage()
    } catch {
      setScraping(false)
    } finally {
      setResumeBusy(false)
    }
  }

  const attachSocket = (id, { reset = false } = {}) => {
    if (!id) return

    // CRITICAL FIX: Strip all event handlers from the old WebSocket BEFORE
    // closing it. Without this, the old socket's onclose fires and schedules
    // a ghost reconnection that immediately kills the NEW good connection,
    // creating the infinite open→close→open→close cascade.
    if (wsRef.current) {
      const oldWs = wsRef.current
      oldWs.onopen = null
      oldWs.onmessage = null
      oldWs.onerror = null
      oldWs.onclose = null
      wsRef.current = null
      try { oldWs.close() } catch (e) { /* already closed */ }
    }

    if (reset) {
      resetLiveBuffers()
      setLeads([])
      setStats({ total: 0, withOwner: 0, withEmail: 0, withWebsite: 0 })
      setRuntimeStatus("")
      retryCountRef.current = 0
    }

    console.log(`[WS] Connecting to /ws/${id} (attempt ${retryCountRef.current})`)
    const ws = new WebSocket(`${WS_BASE}/ws/${id}`)
    wsRef.current = ws

    ws.onopen = () => {
      console.log("[WS] Connection established successfully")
      retryCountRef.current = 0
    }

    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data)
      if (msg.type === "lead") {
        queueLeadForUi(msg.data)
      } else if (msg.type === "url") {
        setLiveUrl(msg.data || { maps_url: "", website_url: "", name: "" })
      } else if (msg.type === "progress") {
        setProgress(msg.data)
      } else if (msg.type === "info") {
        setRuntimeStatus(String(msg.data || ""))
      } else if (msg.type === "proxy_stats") {
        setProxyStats(msg.data || null)
      } else if (msg.type === "block_wait") {
        const d = msg.data || {}
        const wait = Number(d.wait_seconds || 0)
        const attempt = Number(d.retry_attempt || 0)
        const limit = Number(d.retry_limit || 0)
        setRuntimeStatus(`Blocked by Google. Waiting ${wait}s (attempt ${attempt}/${limit}) then auto-resuming...`)
      } else if (msg.type === "done") {
        flushLiveFeed()
        keepSocketAliveRef.current = false
        setScraping(false)
        setCanDownload((msg.data?.total || 0) > 0)
        setRuntimeStatus("Completed")
        clearActiveJob()
        refreshResumeCandidate()
        refreshHistory()
        refreshUsage()
      } else if (msg.type === "error") {
        flushLiveFeed()
        keepSocketAliveRef.current = false
        setScraping(false)
        setRuntimeStatus(String(msg.data || "Stopped with an error"))
        clearActiveJob()
        refreshHistory()
        refreshUsage()
      }
    }

    ws.onerror = (err) => {
      console.error("[WS] Connection error", err)
    }
    
    ws.onclose = (event) => {
      console.log(`[WS] Disconnected (code: ${event.code}, reason: ${event.reason || "none"})`)
      // Only reconnect if THIS socket is still the active one.
      // If attachSocket was called again, wsRef.current would be a different instance,
      // so this stale onclose should NOT trigger reconnection.
      if (ws !== wsRef.current) return

      if (keepSocketAliveRef.current && localStorage.getItem(ACTIVE_JOB_KEY) === id) {
        const timeoutDelay = Math.max(3000, Math.min(3000 * Math.pow(2, retryCountRef.current), 30000))
        retryCountRef.current += 1
        console.log(`[WS] Scheduling reconnect in ${timeoutDelay}ms`)
        setTimeout(async () => {
          // Double-check conditions haven't changed during the delay
          if (!keepSocketAliveRef.current || localStorage.getItem(ACTIVE_JOB_KEY) !== id) return
          const shouldReconnect = await canReconnectJob(id)
          if (!shouldReconnect) {
            keepSocketAliveRef.current = false
            setScraping(false)
            clearActiveJob()
            return
          }
          attachSocket(id)
        }, timeoutDelay)
      }
    }
  }

  // ── Query tag helpers ─────────────────────────────────────
  const addQuery = () => {
    const trimmed = queryInput.trim()
    if (!trimmed) return
    const newTags = trimmed.split(",").map(s => s.trim()).filter(Boolean)
    setQueries(prev => {
      const merged = [...prev]
      newTags.forEach(t => { if (!merged.includes(t)) merged.push(t) })
      return merged
    })
    setQueryInput("")
  }

  const removeQuery = (q) => setQueries(prev => prev.filter(x => x !== q))

  const addArea = () => {
    const trimmed = newArea.trim()
    if (!trimmed) return
    // Support comma separated
    const newAreas = trimmed.split(",").map(s => s.trim()).filter(Boolean)
    newAreas.forEach(a => {
      if (!areas.includes(a)) {
        setAreas(p => [...p, a])
        setSelAreas(p => [...p, a])
      }
    })
    setNewArea("")
  }

  const removeArea = (a) => {
    setAreas(p => p.filter(x => x !== a))
    setSelAreas(p => p.filter(x => x !== a))
  }

  const startScrape = async () => {
    const finalCity    = customCity.trim() || city
    const finalQueries = queries.length ? queries : (customPro || profession ? [customPro || profession] : [])
    const requestedPlatforms = isStarterPlan
      ? { maps: true, justdial: false, indiamart: false }
      : { maps: enableMaps, justdial: enableJD, indiamart: enableIM }
    if (!finalQueries.length || !finalCity) return
    if (!requestedPlatforms.maps && !requestedPlatforms.justdial && !requestedPlatforms.indiamart) return

    keepSocketAliveRef.current = true
    resetLiveBuffers()
    setLeads([]); setScraping(true)
    setProxyStats(null)
    setCanDownload(false)
    setRuntimeStatus("")
    setRuntimeErrorDetails(null)
    setStats({ total: 0, withOwner: 0, withEmail: 0, withWebsite: 0 })
    setProgress({ current: 0, total: finalQueries.length, query: "" })
    try {
      const res = await authRequest(`/scrape/v2/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id:         user.id,
          niche:           `${finalQueries[0]}_${finalCity}`.replace(/ /g, "_").toLowerCase(),
          city:            finalCity,
          queries:         finalQueries,
          enable_maps:     requestedPlatforms.maps,
          enable_justdial: requestedPlatforms.justdial,
          enable_indiamart: requestedPlatforms.indiamart,
          website_filter:  websiteFilter,
          max_per_query:   maxPerQuery,
          use_proxy:       useProxy,
        }),
      })
      if (res.status === 403) {
        const data = await res.json()
        const details = data?.detail && typeof data.detail === "object" ? data.detail : null
        const errorMessage =
          details?.error ||
          (typeof data?.detail === "string" ? data.detail : "") ||
          data?.error ||
          "Upgrade your plan to continue"
        setRuntimeErrorDetails(details)
        setRuntimeStatus(errorMessage)
        setScraping(false)
        await refreshUsage()
        return
      }
      const data = await res.json()
      setJobId(data.job_id)
      localStorage.setItem(ACTIVE_JOB_KEY, data.job_id)
      attachSocket(data.job_id, { reset: true })
    } catch {
      setScraping(false)
      setRuntimeErrorDetails(null)
    }
  }

  const stopScrape = () => {
    flushLiveFeed()
    keepSocketAliveRef.current = false
    setRuntimeStatus("Stopped by user")
    clearActiveJob()
    if (wsRef.current) {
      wsRef.current.onclose = null
      wsRef.current.onerror = null
      wsRef.current.onmessage = null
      wsRef.current.close()
      wsRef.current = null
    }
    if (jobId) authRequest(`/scrape/stop/${jobId}`, { method: "POST" })
    setScraping(false)
    setTimeout(() => {
      setCanDownload(true)
      refreshResumeCandidate()
      refreshUsage()
    }, 1200)
  }

  const downloadCSV = async () => {
    if (!jobId) return
    const res  = await authRequest(`/scrape/download/${jobId}`)
    const blob = await res.blob()
    const url  = URL.createObjectURL(blob)
    const a    = document.createElement("a")
    a.href = url; a.download = `leads_${Date.now()}.csv`; a.click()
    URL.revokeObjectURL(url)
  }

  const deleteJob = async (targetJobId) => {
    if (!confirm("Delete this scrape and all its leads? This cannot be undone.")) return
    try {
      await authRequest(`/scrape/job/${targetJobId}`, {
        method: "DELETE",
        headers: { "x-user-id": user.id },
      })
      refreshHistory()
      refreshResumeCandidate()
      refreshUsage()
      if (targetJobId === jobId) {
        setJobId(null)
        setCanDownload(false)
        setRuntimeStatus("")
        setLeads([])
        setStats({ total: 0, withOwner: 0, withEmail: 0, withWebsite: 0 })
      }
    } catch {}
  }

  const refreshHistory = async () => {
    try {
      const res = await authRequest(`/user/history`)
      if (!res.ok) throw new Error("history failed")
      const data = await res.json()
      setHistory(data.history || [])
    } catch {}
  }

  const pct = progress.total > 0 ? Math.round((progress.current / progress.total) * 100) : 0

  useEffect(() => {
    const savedJobId = localStorage.getItem(ACTIVE_JOB_KEY)
    if (!savedJobId) return

    let mounted = true
    keepSocketAliveRef.current = true
    setJobId(savedJobId)

    authRequest(`/scrape/status/${savedJobId}`)
      .then(r => r.json())
      .then(status => {
        if (!mounted) return
        if (status?.running) {
          setScraping(true)
          if (status.profession) {
            setProfession(status.profession)
            setCustomPro(status.profession)
          }
          if (status.location) {
            if (!areas.includes(status.location)) {
              setAreas(p => {
                const unique = new Set([...p, status.location])
                return Array.from(unique)
              })
            }
            setSelAreas([status.location])
          }
          attachSocket(savedJobId, { reset: true })
        } else {
          setScraping(false)
          setCanDownload((status?.lead_count || 0) > 0)
          clearActiveJob()
        }
      })
      .catch(() => {
        if (mounted) clearActiveJob()
      })

    return () => {
      mounted = false
      keepSocketAliveRef.current = false
      resetLiveBuffers()
      if (wsRef.current) {
        wsRef.current.onclose = null
        wsRef.current.onerror = null
        wsRef.current.onmessage = null
        wsRef.current.close()
        wsRef.current = null
      }
    }
  }, [ACTIVE_JOB_KEY])

  useEffect(() => {
    refreshResumeCandidate()
    refreshHistory()
    refreshUsage()
  }, [user.id])

  // Sleep/wake detection — auto-reconnect WebSocket when laptop resumes
  useEffect(() => {
    const handleVisibility = async () => {
      if (document.visibilityState === "visible") {
        const savedJobId = localStorage.getItem(ACTIVE_JOB_KEY)
        if (!savedJobId) return
        try {
          const res = await authRequest(`/scrape/heartbeat/${savedJobId}`)
          const hb = await res.json()
          if (hb.alive && hb.status === "running") {
            // Reconnect the WebSocket
            keepSocketAliveRef.current = true
            setScraping(true)
            attachSocket(savedJobId)
          } else {
            // Job died while sleeping
            keepSocketAliveRef.current = false
            setScraping(false)
            setCanDownload(true)
            clearActiveJob()
            refreshResumeCandidate()
            refreshHistory()
            refreshUsage()
          }
        } catch {
          // Backend unreachable
          keepSocketAliveRef.current = false
          setScraping(false)
          clearActiveJob()
        }
      }
    }
    document.addEventListener("visibilitychange", handleVisibility)
    return () => document.removeEventListener("visibilitychange", handleVisibility)
  }, [])

  // Heartbeat polling — detect if scrape died while running
  useEffect(() => {
    if (scraping && jobId) {
      heartbeatRef.current = setInterval(async () => {
        try {
          const res = await authRequest(`/scrape/heartbeat/${jobId}`)
          const hb = await res.json()
          if (!hb.alive || hb.status !== "running") {
            flushLiveFeed()
            keepSocketAliveRef.current = false
            setScraping(false)
            setCanDownload(true)
            clearActiveJob()
            refreshResumeCandidate()
            refreshHistory()
            refreshUsage()
          }
        } catch {}
      }, 15000) // every 15 seconds
    } else {
      if (heartbeatRef.current) clearInterval(heartbeatRef.current)
    }
    return () => { if (heartbeatRef.current) clearInterval(heartbeatRef.current) }
  }, [scraping, jobId])

  const chipStyle = (selected) => ({
    padding: "4px 10px", fontSize: 11, borderRadius: 100, cursor: "pointer",
    fontFamily: "var(--font-display)", transition: "all 0.15s",
    border: `1px solid ${selected ? "var(--accent-cyan)" : "var(--border)"}`,
    background: selected ? "var(--accent-cyan-dim)" : "transparent",
    color: selected ? "var(--accent-cyan)" : "var(--text-muted)",
  })
  const usageLimit = Number(planLimits?.leads || 0)
  const usageCount = usage?.leads_used_this_month || 0
  const usagePct = usageLimit > 0 ? Math.min(100, Math.round((usageCount / usageLimit) * 100)) : 0
  const usageWarning = usagePct >= 100 ? "hard" : usagePct >= 80 ? "soft" : "none"

  return (
    <div className="page">
      <SparklesBg />
      <Nav user={user} onLogout={onLogout} />
      <div style={{ paddingTop: 64, minHeight: "100vh" }}>
        <div style={{ maxWidth: 1400, margin: "0 auto", padding: "32px 24px" }}>
          <div style={{ marginBottom: 28 }} className="anim-fade-up">
            <h1 style={{ fontSize: 28, fontWeight: 800, letterSpacing: "-0.02em", marginBottom: 4 }}>Lead Scraper</h1>
            <p style={{ color: "var(--text-secondary)", fontSize: 14 }}>Configure target and launch — every lead auto-enriched</p>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "380px 1fr", gap: 24, alignItems: "start" }}>

            {/* ── LEFT PANEL ── */}
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }} className="stagger">

              {/* Location card */}
              <div className="card">
                <p style={{ fontSize: 10, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--text-muted)", marginBottom: 16 }}>Target Location</p>

                {/* Country searchable dropdown */}
                <div style={{ marginBottom: 14 }} ref={countryRef}>
                  <label>Country ({allCountries.length} available)</label>
                  <div style={{ position: "relative" }}>
                    <input
                      placeholder="Search any country..."
                      value={countrySearch}
                      onChange={e => { setCountrySearch(e.target.value); setShowCountryDD(true) }}
                      onFocus={() => setShowCountryDD(true)}
                    />
                    {showCountryDD && (
                      <div style={{ position: "absolute", top: "100%", left: 0, right: 0, background: "var(--bg-card)", border: "1px solid var(--border)", borderRadius: "var(--radius-md)", maxHeight: 220, overflowY: "auto", zIndex: 50, marginTop: 4 }}>
                        {filteredCountries.length === 0 ? (
                          <div style={{ padding: "12px 14px", fontSize: 12, color: "var(--text-muted)" }}>No countries found</div>
                        ) : filteredCountries.map(c => (
                          <div key={c} onClick={() => { setCountry(c); setCity(""); setAreas([]); setSelAreas([]); setCountrySearch(c); setShowCountryDD(false) }}
                            style={{ padding: "10px 14px", fontSize: 13, cursor: "pointer", color: country === c ? "var(--accent-cyan)" : "var(--text-primary)", background: country === c ? "var(--accent-cyan-dim)" : "transparent" }}
                            onMouseEnter={e => e.currentTarget.style.background = "var(--bg-surface)"}
                            onMouseLeave={e => e.currentTarget.style.background = country === c ? "var(--accent-cyan-dim)" : "transparent"}
                          >{c}</div>
                        ))}
                      </div>
                    )}
                  </div>
                </div>

                {/* City */}
                {country && cities.length > 0 && (
                  <div style={{ marginBottom: 14 }}>
                    <label>City / Region</label>
                    <select value={city} onChange={e => setCity(e.target.value)}>
                      <option value="">Select city</option>
                      {cities.map(c => <option key={c}>{c}</option>)}
                    </select>
                  </div>
                )}

                {/* Areas */}
                {areas.length > 0 && (
                  <div style={{ marginBottom: 14 }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                      <label style={{ margin: 0 }}>Areas ({selAreas.length}/{areas.length})</label>
                      <div style={{ display: "flex", gap: 6 }}>
                        <button className="btn btn-ghost" style={{ padding: "3px 10px", fontSize: 11 }} onClick={() => setSelAreas([...areas])}>All</button>
                        <button className="btn btn-ghost" style={{ padding: "3px 10px", fontSize: 11 }} onClick={() => setSelAreas([])}>None</button>
                      </div>
                    </div>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 6, maxHeight: 200, overflowY: "auto", padding: "4px 0" }}>
                      {areas.map(a => (
                        <div key={a} style={{ display: "flex", alignItems: "center", gap: 2 }}>
                          <button onClick={() => toggle(a)} style={chipStyle(selAreas.includes(a))}>{a}</button>
                          <button onClick={() => removeArea(a)}
                            style={{ background: "none", border: "none", color: "var(--text-muted)", cursor: "pointer", fontSize: 14, padding: "0 2px", lineHeight: 1 }}
                            title="Remove area">×</button>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Add custom area */}
                <div>
                  <label>Add area / unknown place</label>
                  <div style={{ display: "flex", gap: 8 }}>
                    <input
                      placeholder="e.g. Bhatkal, Duqm, Any Town"
                      value={newArea}
                      onChange={e => setNewArea(e.target.value)}
                      onKeyDown={e => e.key === "Enter" && addArea()}
                      style={{ flex: 1 }}
                    />
                    <button className="btn btn-ghost" style={{ padding: "8px 14px", flexShrink: 0, fontSize: 12 }} onClick={addArea}>
                      + Add
                    </button>
                  </div>
                  <p style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 5 }}>
                    Separate multiple with commas. Works for any location worldwide. Press Enter or click Add.
                  </p>
                </div>

                {selAreas.length > 0 && (
                  <div style={{ marginTop: 10, fontSize: 11, color: "var(--text-muted)" }}>
                    ~{selAreas.length * 100} estimated leads
                  </div>
                )}
              </div>

              {/* Search Queries card */}
              <div className="card">
                <p style={{ fontSize: 10, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--text-muted)", marginBottom: 14 }}>Search Queries</p>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 12 }}>
                  {PROFESSIONS.map(p => (
                    <button key={p}
                      onClick={() => setQueries(prev => prev.includes(p) ? prev.filter(x => x !== p) : [...prev, p])}
                      style={{ padding: "4px 10px", fontSize: 11, borderRadius: 100, cursor: "pointer", fontFamily: "var(--font-display)", transition: "all 0.15s",
                        border: `1px solid ${queries.includes(p) ? "var(--accent-violet)" : "var(--border)"}`,
                        background: queries.includes(p) ? "var(--accent-violet-dim)" : "transparent",
                        color: queries.includes(p) ? "var(--accent-violet)" : "var(--text-muted)" }}>
                      {p}
                    </button>
                  ))}
                </div>
                {queries.length > 0 && (
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 10 }}>
                    {queries.map(q => (
                      <span key={q} style={{ display: "inline-flex", alignItems: "center", gap: 4, padding: "3px 10px", fontSize: 11, borderRadius: 100,
                        background: "var(--accent-violet-dim)", border: "1px solid var(--accent-violet)", color: "var(--accent-violet)" }}>
                        {q}
                        <button onClick={() => removeQuery(q)}
                          style={{ background: "none", border: "none", color: "var(--accent-violet)", cursor: "pointer", fontSize: 13, lineHeight: 1, padding: 0 }}>×</button>
                      </span>
                    ))}
                  </div>
                )}
                <div style={{ display: "flex", gap: 8 }}>
                  <input placeholder="Add query, e.g. gyms, clinics..." value={queryInput} onChange={e => setQueryInput(e.target.value)}
                    onKeyDown={e => e.key === "Enter" && addQuery()} style={{ flex: 1 }} />
                  <button className="btn btn-ghost" style={{ padding: "8px 14px", flexShrink: 0, fontSize: 12 }} onClick={addQuery}>+ Add</button>
                </div>
                <p style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 5 }}>Separate multiple with commas or click preset chips above.</p>
              </div>

              {/* City override card */}
              <div className="card">
                <p style={{ fontSize: 10, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--text-muted)", marginBottom: 10 }}>City Override</p>
                <label>Custom city (leave blank to use selector above)</label>
                <input placeholder={city || "e.g. Mumbai, Delhi, Chennai…"} value={customCity} onChange={e => setCustomCity(e.target.value)} />
                {(customCity || city) && (
                  <p style={{ fontSize: 11, color: "var(--accent-cyan)", marginTop: 6 }}>
                    Scraping in: <strong>{customCity.trim() || city}</strong>
                  </p>
                )}
              </div>

              {/* Platforms & Filters card */}
              <div className="card">
                <p style={{ fontSize: 10, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--text-muted)", marginBottom: 14 }}>Platforms & Filters</p>

                <label style={{ marginBottom: 8, display: "block" }}>Platforms</label>
                <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
                  {[["Maps", enableMaps, setEnableMaps, "var(--accent-cyan)", false],
                    ["JustDial", enableJD, setEnableJD, "var(--accent-violet)", isStarterPlan],
                    ["IndiaMart", enableIM, setEnableIM, "var(--accent-gold)", isStarterPlan]].map(([label, on, setter, color, locked]) => (
                    <button key={label} onClick={() => !locked && setter(v => !v)}
                      disabled={locked}
                      style={{ flex: 1, padding: "8px 4px", fontSize: 12, borderRadius: "var(--radius-md)", transition: "all 0.15s",
                        border: `1px solid ${on ? color : "var(--border)"}`,
                        background: on ? `${color}22` : "transparent",
                        color: on ? color : "var(--text-muted)",
                        fontFamily: "var(--font-display)", fontWeight: on ? 700 : 400,
                        opacity: locked ? 0.45 : 1,
                        cursor: locked ? "not-allowed" : "pointer" }}>
                      {on ? "✓ " : ""}{label}
                    </button>
                  ))}
                </div>
                {isStarterPlan && (
                  <p style={{ fontSize: 11, color: "var(--text-muted)", marginTop: -8, marginBottom: 12 }}>
                    Starter plan includes Google Maps only.
                  </p>
                )}

                <label style={{ marginBottom: 6, display: "block" }}>Website Filter</label>
                <select value={websiteFilter} onChange={e => setWebsiteFilter(e.target.value)} style={{ marginBottom: 16 }}>
                  <option value="no_website">no_website — zero web presence (best leads)</option>
                  <option value="minimal">minimal — no/thin website (recommended)</option>
                  <option value="all">all — no filter, maximum output</option>
                </select>

                <label style={{ marginBottom: 6, display: "block" }}>
                  Max results per query/platform: <strong style={{ color: "var(--accent-cyan)" }}>{maxPerQuery}</strong>
                </label>
                <input type="range" min={5} max={100} step={5} value={maxPerQuery}
                  onChange={e => setMaxPerQuery(Number(e.target.value))}
                  style={{ width: "100%", marginBottom: 16, accentColor: "var(--accent-cyan)" }} />

                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <label style={{ margin: 0 }}>Auto-Rotating Proxy Pool</label>
                  <button onClick={() => setUseProxy(v => !v)}
                    style={{ padding: "4px 14px", fontSize: 11, borderRadius: 100, cursor: "pointer", transition: "all 0.15s",
                      border: `1px solid ${useProxy ? "var(--accent-green)" : "var(--border)"}`,
                      background: useProxy ? "var(--accent-green-dim, #00ff8822)" : "transparent",
                      color: useProxy ? "var(--accent-green)" : "var(--text-muted)" }}>
                    {useProxy ? "ON" : "OFF"}
                  </button>
                </div>
                {useProxy && !proxyStats && (
                  <p style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 6 }}>Pool loads when scraping starts (~30-60s)</p>
                )}
                {proxyStats && (
                  <div style={{ marginTop: 10, padding: "10px 14px", background: "var(--bg-surface)", borderRadius: "var(--radius-sm)", fontSize: 12 }}>
                    <div style={{ color: "var(--accent-green)", fontWeight: 600, marginBottom: 4 }}>
                      Proxy pool live — {proxyStats.live} proxies
                    </div>
                    <div style={{ color: "var(--text-muted)" }}>
                      Fastest: {proxyStats.fastest_ms}ms &nbsp;|&nbsp;
                      {Object.entries(proxyStats.by_protocol || {}).map(([k, v]) => `${k}:${v}`).join(" · ")}
                    </div>
                  </div>
                )}
              </div>

              {/* Launch */}
              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                {!scraping ? (
                  <button className="btn btn-primary" style={{ width: "100%", padding: 14, fontSize: 14, justifyContent: "center" }}
                    onClick={startScrape}
                    disabled={!queries.length || !(customCity.trim() || city) || (isStarterPlan ? false : (!enableMaps && !enableJD && !enableIM)) || usagePct >= 100}>
                    Launch scraper →
                  </button>
                ) : (
                  <button className="btn btn-danger" style={{ width: "100%", padding: 14, fontSize: 14, justifyContent: "center" }} onClick={stopScrape}>
                    ⏹ Stop scraping
                  </button>
                )}
                {!scraping && resumeCandidate && (
                  <>
                    <button className="btn btn-ghost" style={{ width: "100%", padding: 12, fontSize: 13, justifyContent: "center" }} onClick={() => resumeScrape(false)} disabled={resumeBusy}>
                      {resumeBusy ? "Resuming..." : `Resume stopped job (${resumeCandidate.remaining} areas left)`}
                    </button>
                    <button className="btn btn-ghost" style={{ width: "100%", padding: 12, fontSize: 12, justifyContent: "center" }} onClick={() => resumeScrape(true)} disabled={resumeBusy}>
                      Restart that job from beginning
                    </button>
                  </>
                )}
                {jobId && canDownload && !scraping && (
                  <button className="btn btn-ghost" style={{ width: "100%", padding: 12, fontSize: 13, justifyContent: "center" }} onClick={downloadCSV}>
                    ↓ Download CSV
                  </button>
                )}
              </div>
            </div>

            {/* ── RIGHT PANEL ── */}
            <div style={{ display: "flex", flexDirection: "column", gap: 16 }} className="stagger">
              {usage && (
                <div className="card">
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10, gap: 16, flexWrap: "wrap" }}>
                    <div>
                      <div style={{ fontSize: 14, fontWeight: 700 }}>
                        Leads this month: {usageCount} / {usageLimit}{" "}
                        {usageWarning !== "none" && (
                          <span style={{ color: usageWarning === "hard" ? "var(--accent-red)" : "var(--accent-gold)" }}>
                            — {usageWarning === "hard" ? "Upgrade to Pro" : "Upgrade soon"}
                          </span>
                        )}
                      </div>
                      <div style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 4 }}>
                        Plan: {activePlan.toUpperCase()} · Searches today: {usage.searches_today}
                        {planLimits?.searches !== null && planLimits?.searches !== undefined && Number.isFinite(planLimits.searches) ? ` / ${planLimits.searches}` : " / Unlimited"}
                      </div>
                    </div>
                    <button className="btn btn-ghost" style={{ padding: "8px 14px" }} onClick={() => navigate("/pricing")}>
                      Upgrade
                    </button>
                  </div>
                  <div className="progress-bar">
                    <div
                      className="progress-fill"
                      style={{
                        width: `${usagePct}%`,
                        background: usageWarning === "hard" ? "rgba(255,100,100,0.9)" : usageWarning === "soft" ? "rgba(255,255,255,0.7)" : "#FFFFFF",
                      }}
                    />
                  </div>
                </div>
              )}

              <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 12 }}>
                {[["Total Leads",stats.total,"var(--accent-cyan)"],["With Owner",stats.withOwner,"var(--accent-violet)"],["With Email",stats.withEmail,"var(--accent-gold)"],["With Website",stats.withWebsite,"var(--accent-green)"]].map(([l,v,c],i) => (
                  <div key={i} className="card" style={{ padding: "16px 20px" }}>
                    <div style={{ fontSize: 10, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 4 }}>{l}</div>
                    <div style={{ fontSize: 32, fontWeight: 800, color: c, letterSpacing: "-0.02em", fontFamily: "var(--font-mono)" }}>{v}</div>
                  </div>
                ))}
              </div>

              {(scraping || progress.total > 0) && (
                <div className="card">
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                      {scraping && <span className="stat-pill"><span className="dot" /> Running</span>}
                      <span style={{ fontSize: 13, color: "var(--text-secondary)" }}>{progress.query || "Initializing..."}</span>
                    </div>
                    <span style={{ fontSize: 13, fontFamily: "var(--font-mono)", color: "var(--accent-cyan)" }}>{progress.current}/{progress.total}</span>
                  </div>
                  <div className="progress-bar"><div className="progress-fill" style={{ width: `${pct}%` }} /></div>
                  <div style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 6, textAlign: "right" }}>{pct}% complete</div>
                  {!!runtimeStatus && <div style={{ marginTop: 8, fontSize: 12, color: "var(--accent-gold)" }}>{runtimeStatus}</div>}
                  {scraping && liveUrl.name && (
                    <div style={{ marginTop: 10, padding: "10px 14px", background: "var(--bg-surface)", borderRadius: "var(--radius-sm)", fontSize: 12 }}>
                      <div style={{ color: "var(--accent-cyan)", fontWeight: 600, marginBottom: 4 }}>Now scraping: {liveUrl.name}</div>
                      {liveUrl.maps_url && (
                        <a href={liveUrl.maps_url} target="_blank" rel="noreferrer" style={{ color: "var(--text-muted)", fontSize: 11, textDecoration: "none", wordBreak: "break-all" }}>
                          📍 {liveUrl.maps_url.substring(0, 80)}...
                        </a>
                      )}
                      {liveUrl.website_url && (
                        <div style={{ marginTop: 4 }}>
                          <a href={liveUrl.website_url} target="_blank" rel="noreferrer" style={{ color: "var(--text-muted)", fontSize: 11, textDecoration: "none" }}>
                            🌐 {liveUrl.website_url.substring(0, 60)}
                          </a>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}

              <div className="card" style={{ padding: 0, overflow: "hidden" }}>
                <div style={{ padding: "16px 20px", borderBottom: "1px solid var(--border)", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <p style={{ fontSize: 13, fontWeight: 600 }}>Live Feed {scraping && <span style={{ fontSize: 11, color: "var(--accent-green)", marginLeft: 6 }}>● incoming</span>}</p>
                  {stats.total > 0 && <span style={{ fontSize: 11, color: "var(--text-muted)", fontFamily: "var(--font-mono)" }}>{stats.total} collected</span>}
                </div>
                <div ref={tableRef} style={{ maxHeight: 480, overflowY: "auto" }}>
                  {leads.length === 0 ? (
                    <div style={{ padding: "60px 20px", textAlign: "center" }}>
                      <div style={{ fontSize: 40, opacity: 0.15, marginBottom: 12 }}>◈</div>
                      <p style={{ color: "var(--text-muted)", fontSize: 13 }}>
                        {scraping ? "Scraping — leads appear here in real time..." : "Configure and launch to start collecting leads"}
                      </p>
                    </div>
                  ) : (
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Business</th>
                          <th>Phone</th>
                          <th>Email</th>
                          <th>Website</th>
                          <th>Web Status</th>
                          <th>Source</th>
                          <th>Listing</th>
                        </tr>
                      </thead>
                      <tbody>
                        {leads.map((lead,i) => {
                          const wsColor = {
                            no_website:  "var(--accent-gold)",
                            social_only: "var(--accent-violet)",
                            minimal:     "var(--accent-cyan)",
                            full:        "var(--accent-green)",
                            unreachable: "var(--text-muted)",
                          }[lead.website_status] || "var(--text-muted)"
                          const srcColor = {
                            google_maps: "var(--accent-cyan)",
                            justdial:    "var(--accent-violet)",
                            indiamart:   "var(--accent-gold)",
                          }[lead.source] || "var(--text-muted)"
                          const listingUrl = lead.listing_url || lead["Maps URL"] || ""
                          return (
                            <tr key={i}>
                              <td>
                                <div style={{ fontWeight: 500 }}>{lead.Name||"—"}</div>
                                <div style={{ fontSize:10,color:"var(--text-muted)" }}>{lead.Category||lead.category}</div>
                              </td>
                              <td>{lead.Phone||lead.phone||"—"}</td>
                              <td style={{ maxWidth:140,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>
                                {lead.Email||lead.email||lead.Owner_Email_Guesses?.split(" | ")[0]||"—"}
                              </td>
                              <td>
                                {(lead.Website||lead.website) ? (
                                  <a href={lead.Website||lead.website} target="_blank" rel="noreferrer"
                                    className="badge badge-green" style={{ textDecoration:"none" }}>
                                    Visit ↗
                                  </a>
                                ) : (
                                  <span className="badge badge-red">None</span>
                                )}
                              </td>
                              <td>
                                <span style={{ fontSize:11, color: wsColor, fontWeight:600 }}>
                                  {lead.website_status || "—"}
                                </span>
                              </td>
                              <td>
                                <span style={{ fontSize:11, color: srcColor, fontWeight:600 }}>
                                  {lead.source || "—"}
                                </span>
                              </td>
                              <td>
                                {listingUrl ? (
                                  <a href={listingUrl} target="_blank" rel="noreferrer"
                                    style={{ fontSize:11, color:"var(--text-muted)", textDecoration:"none" }}>
                                    ↗
                                  </a>
                                ) : "—"}
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>

              {/* Scrape History */}
              <div className="card" style={{ padding: 0, overflow: "hidden" }}>
                <div
                  style={{ padding: "16px 20px", borderBottom: "1px solid var(--border)", display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer" }}
                  onClick={() => { setShowHistory(!showHistory); if (!showHistory) refreshHistory() }}
                >
                  <p style={{ fontSize: 13, fontWeight: 600 }}>Scrape History</p>
                  <span style={{ fontSize: 11, color: "var(--text-muted)" }}>{showHistory ? "▼" : "▶"} {history.length} jobs</span>
                </div>
                {showHistory && (
                  <div style={{ maxHeight: 360, overflowY: "auto" }}>
                    {history.length === 0 ? (
                      <div style={{ padding: "30px 20px", textAlign: "center", color: "var(--text-muted)", fontSize: 13 }}>No scrape jobs yet</div>
                    ) : history.map((h) => (
                      <div key={h.job_id} style={{ padding: "12px 20px", borderBottom: "1px solid var(--border)", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontSize: 13, fontWeight: 500, color: "var(--text-primary)" }}>
                            {h.profession} — {h.location || "Unknown"}
                          </div>
                          <div style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 2 }}>
                            {h.effective_lead_count || h.lead_count || 0} leads · {h.processed_areas || 0}/{h.total_areas || "?"} areas ·{" "}
                            <span className={`badge ${h.status === "completed" ? "badge-green" : h.status === "stopped" ? "badge-gold" : "badge-red"}`}>
                              {h.status}
                            </span>
                          </div>
                        </div>
                        <div style={{ display: "flex", gap: 6 }}>
                          {(h.effective_lead_count || h.lead_count || 0) > 0 && (
                            <button
                              className="btn btn-ghost"
                              style={{ padding: "4px 10px", fontSize: 11 }}
                              onClick={async () => {
                                const res = await authRequest(`/scrape/download/${h.job_id}`)
                                const blob = await res.blob()
                                const url = URL.createObjectURL(blob)
                                const a = document.createElement("a")
                                a.href = url; a.download = `leads_${h.niche || "job"}.csv`; a.click()
                                URL.revokeObjectURL(url)
                              }}
                            >↓ CSV</button>
                          )}
                          <button
                            className="btn btn-danger"
                            style={{ padding: "4px 10px", fontSize: 11 }}
                            onClick={() => deleteJob(h.job_id)}
                          >🗑</button>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
