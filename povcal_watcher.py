import requests
import re
import pandas as pd
import os
import datetime
import pdb


def fetch_data():
    """Fetch data from PovCalNet at $1.90 poverty line."""
    url = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx"
    params = {
        "C0": "ALB_3",
        "C1": "DZA_3",
        "C2": "AGO_3",
        "C3": "AGO_2",
        "C4": "ARG_2",
        "C5": "ARM_3",
        "C6": "AUS_3",
        "C7": "AUT_3",
        "C8": "AZE_3",
        "C9": "BGD_3",
        "C10": "BLR_3",
        "C11": "BEL_3",
        "C12": "BLZ_3",
        "C13": "BEN_3",
        "C14": "BTN_3",
        "C15": "BOL_3",
        "C16": "BOL_2",
        "C17": "BIH_3",
        "C18": "BWA_3",
        "C19": "BRA_3",
        "C20": "BGR_3",
        "C21": "BFA_3",
        "C22": "BDI_3",
        "C23": "CPV_3",
        "C24": "CMR_3",
        "C25": "CAN_3",
        "C26": "CAF_3",
        "C27": "TCD_3",
        "C28": "CHL_3",
        "C29": "CHN_5",
        "C30": "CHN_1",
        "C31": "CHN_2",
        "C32": "COL_3",
        "C33": "COL_2",
        "C34": "COM_3",
        "C35": "ZAR_3",
        "C36": "COG_3",
        "C37": "CRI_3",
        "C38": "CIV_3",
        "C39": "HRV_3",
        "C40": "CYP_3",
        "C41": "CZE_3",
        "C42": "DNK_3",
        "C43": "DJI_3",
        "C44": "DOM_3",
        "C45": "ECU_3",
        "C46": "ECU_2",
        "C47": "EGY_3",
        "C48": "SLV_3",
        "C49": "EST_3",
        "C50": "ETH_3",
        "C51": "ETH_1",
        "C52": "FJI_3",
        "C53": "FIN_3",
        "C54": "FRA_3",
        "C55": "GAB_3",
        "C56": "GMB_3",
        "C57": "GEO_3",
        "C58": "DEU_3",
        "C59": "GHA_3",
        "C60": "GRC_3",
        "C61": "GTM_3",
        "C62": "GIN_3",
        "C63": "GNB_3",
        "C64": "GUY_3",
        "C65": "HTI_3",
        "C66": "HND_3",
        "C67": "HND_2",
        "C68": "HUN_3",
        "C69": "ISL_3",
        "C70": "IND_5",
        "C71": "IND_1",
        "C72": "IND_2",
        "C73": "IDN_5",
        "C74": "IDN_1",
        "C75": "IDN_2",
        "C76": "IRN_3",
        "C77": "IRQ_3",
        "C78": "IRL_3",
        "C79": "ISR_3",
        "C80": "ITA_3",
        "C81": "JAM_3",
        "C82": "JPN_3",
        "C83": "JOR_3",
        "C84": "KAZ_3",
        "C85": "KEN_3",
        "C86": "KIR_3",
        "C87": "KOR_3",
        "C88": "KSV_3",
        "C89": "KGZ_3",
        "C90": "LAO_3",
        "C91": "LVA_3",
        "C92": "LBN_3",
        "C93": "LSO_3",
        "C94": "LBR_3",
        "C95": "LTU_3",
        "C96": "LUX_3",
        "C97": "MKD_3",
        "C98": "MDG_3",
        "C99": "MWI_3",
        "C100": "MYS_3",
        "C101": "MDV_3",
        "C102": "MLI_3",
        "C103": "MRT_3",
        "C104": "MUS_3",
        "C105": "MEX_3",
        "C106": "FSM_3",
        "C107": "FSM_2",
        "C108": "MDA_3",
        "C109": "MNG_3",
        "C110": "MNE_3",
        "C111": "MAR_3",
        "C112": "MOZ_3",
        "C113": "MMR_3",
        "C114": "NAM_3",
        "C115": "NPL_3",
        "C116": "NLD_3",
        "C117": "NIC_3",
        "C118": "NER_3",
        "C119": "NGA_3",
        "C120": "NOR_3",
        "C121": "PAK_3",
        "C122": "PAN_3",
        "C123": "PNG_3",
        "C124": "PRY_3",
        "C125": "PER_3",
        "C126": "PHL_3",
        "C127": "POL_3",
        "C128": "PRT_3",
        "C129": "ROU_3",
        "C130": "RUS_3",
        "C131": "RWA_3",
        "C132": "WSM_3",
        "C133": "STP_3",
        "C134": "SEN_3",
        "C135": "SRB_3",
        "C136": "SYC_3",
        "C137": "SLE_3",
        "C138": "SVK_3",
        "C139": "SVN_3",
        "C140": "SLB_3",
        "C141": "ZAF_3",
        "C142": "SSD_3",
        "C143": "ESP_3",
        "C144": "LKA_3",
        "C145": "LCA_3",
        "C146": "SDN_3",
        "C147": "SUR_3",
        "C148": "SWZ_3",
        "C149": "SWE_3",
        "C150": "CHE_3",
        "C151": "SYR_3",
        "C152": "TJK_3",
        "C153": "TZA_3",
        "C154": "THA_3",
        "C155": "TMP_3",
        "C156": "TGO_3",
        "C157": "TON_3",
        "C158": "TTO_3",
        "C159": "TUN_3",
        "C160": "TUR_3",
        "C161": "TKM_3",
        "C162": "TUV_3",
        "C163": "UGA_3",
        "C164": "UKR_3",
        "C165": "GBR_3",
        "C166": "USA_3",
        "C167": "URY_3",
        "C168": "URY_2",
        "C169": "UZB_3",
        "C170": "VUT_3",
        "C171": "VEN_3",
        "C172": "VNM_3",
        "C173": "WBG_3",
        "C174": "YEM_3",
        "C175": "ZMB_3",
        "C176": "ZWE_3",
        "Countries": "all",
        "GroupedBy": "WB",
        "PovertyLine": "1.9",
        "YearSelected": "2013",
        "format": "js"
    }
    response = requests.post(url=url, params=params)

    raw_content = str(response.content)

    agg = re.findall('aggrItem\((.*?)\)', raw_content)
    smy = re.findall('smyItem\((.*?)\)', raw_content)

    agg_format = "[{}]".format(",".join(["[{}]".format(row) for row in agg])).replace("\\", "")
    smy_format = "[{}]".format(",".join(["[{}]".format(row) for row in smy])).replace("\\", "")

    agg_data = pd.read_json(agg_format)
    agg_data.columns = ["RequestYear", "RegionTitle", "RegionCID", "PovertyLine", "Mean", "H", "PG", "P2", "Populations"]
    smy_data = pd.read_json(smy_format)
    smy_data.columns = ["isConsolidated", "displayMode", "useMicroData", "CountryCode", "CountryName", "RegionCID", "CoverageType", "RequestYear", "DataType", "PPP", "PovertyLine", "Mean", "H", "PG", "P2", "watts", "gini", "median", "mld", "pol", "rmed", "rmhalf", "ris", "IA", "Populations", "DataYear", "SvyInfoID", "PPPStatus", "Deciles", "Unknown"]

    smy_data["Deciles"] = smy_data["Deciles"].map(lambda x: [l for l in x] if x is not None else [None]*10)
    decile_df = pd.DataFrame(smy_data["Deciles"].tolist())
    decile_df.columns = ["Decile1", "Decile2", "Decile3", "Decile4", "Decile5", "Decile6", "Decile7", "Decile8", "Decile9", "Decile10", ]
    smy_data = smy_data.drop("Deciles", axis=1)
    smy_data = smy_data.join(decile_df, how="outer")
    return agg_data, smy_data


def record_data(agg_data, smy_data):
    """If data is new, record it to hard disk."""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    new_dir = os.path.join(dir_path, datetime.datetime.now().strftime('%Y-%m-%d'))
    os.makedirs(new_dir)
    agg_data.to_pickle(os.path.join(new_dir, "agg.pkl"))
    smy_data.to_pickle(os.path.join(new_dir, "smy.pkl"))


def data_has_not_changed(new_agg, new_smy):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    all_subdirs = [d for d in os.listdir(dir_path) if os.path.isdir(d)]
    latest_subdir = max(all_subdirs, key=os.path.getmtime)
    old_agg = pd.read_pickle(os.path.join(dir_path, latest_subdir, "agg.pkl"))
    old_smy = pd.read_pickle(os.path.join(dir_path, latest_subdir, "smy.pkl"))
    return new_agg.equals(old_agg) and new_smy.equals(old_smy)


def main():
    agg_data, smy_data = fetch_data()
    if data_has_not_changed(agg_data, smy_data):
        print("Data has not changed.")
    else:
        record_data(agg_data, smy_data)
        print("Data has changed.")


if __name__ == '__main__':
    main()
