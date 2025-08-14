import json
import re
import time
from time import sleep

import pandas as pd
import requests
from playwright.sync_api import sync_playwright
from typing import Dict, List, Optional
from retry import retry
from requests.exceptions import RequestException
# ======================== 配置常量 ========================
COOKIES = {
    'TealeafAkaSid': 'dnqslubE9VbkvegGBruFN1HIvurU-21Z',
    'sapphire': '1',
    '3YCzT93n': 'A6pVlpGYAQAA2ZsuaA5e-QJiOhkgoa2JrUwaILGWRL2Ap53tyHpyCwLXb9vwAWeXrCf6Ky9GfMBeCOfvosJeCA|1|1|03cf0d7aeda20dff2c225f152b70f4f8cea9caba',
    'fiatsCookie': 'DSI_2109|DSN_Hialeah|DSZ_33012',
    '_px3': '1ccc27c2e1d717d8ca9065dae4ee43c86a46fe1f252eb1bbac69d4ff047ff804:ZMNhNC54xm8Gw7zH/I+ITB2Zl4EL/1H2xkLrcQVj4mux74Ix6lSSSJnl/J2YYWE5OOfBa/inDOgIyXt5thNqQw==:1000:oJP81kDNC3lvcQrPLLbMPK4knjdMnOnTAH37FsVXkb1g9wWuRsHqSHCEcvKx5o3QJksFZ5SFc07DRgdfsRJaeh7US/KF8Ug9M8DimeejTPz8V4Gh4aWqkI546K5mTIzmgKKyITeOcbTanFFjd6llrnrEYD2lnRksJs8M3hUaRQVXXGee1P2xO+y8okmG71F/1xX2waP6w478IlqFS9n9lavx+8gSe5kOeuf2rlDSytA=',
    'hasApp': 'true',
    'loyaltyid': 'tly.433f56f1b31140de9f5dea241eb407f7',
    'mid': '10092084020',
    'profileCreatedDate': '2024-08-27T15:26:48.142Z',
    'sapphire_audiences': '{%22base_membership%22:true%2C%22card_membership%22:false%2C%22paid_membership%22:false}',
    'stateprivacycontrols': 'N',
    'usprivacy': '1NN-',
    'accessToken': 'eyJraWQiOiJlYXMyIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiIxMDA5MjA4NDAyMCIsImlzcyI6Ik1JNiIsImV4cCI6MTc1NDgwMzc0MiwiaWF0IjoxNzU0Nzg5MzQyLCJqdGkiOiJUR1QuOGRkNWIxOTExMzc4NDM3YzhkZGM3YTcwMzA1NDQwYTItbSIsInNreSI6ImVhczIiLCJzdXQiOiJSIiwiZGlkIjoiMzc1ZjhiYTQ0M2MyZGE5ZDQ0M2M5MTIxOTgxZTQ5NTQ0YzllMjRlNjM2NTQ1YWVkZjJjNGNlYTc1NjVmYWZjMCIsImVpZCI6Im1ma2FkaHd0eHdAb3V0bG9vay5jb20iLCJzY28iOiJlY29tLm1lZCxvcGVuaWQiLCJjbGkiOiJlY29tLXdlYi0xLjAuMCIsInR2MSI6IjExMTAwNjEzMTMiLCJhc2wiOiJNIn0.vVpP3AyFK8tVs-U5OvJl-KHZlXjIPNBuyXRi5PYJADx7GkstZzLNNtLZidtkORrDWTn1EtpJk23rNI35GEaj1sC4CP2qD2sx7qtX7unRc_f8N2qT1Fj3BL7lY0w23zXxc1h0f6Xx9lHQGoLS95P1VovUP9ck6Pj7RzgRG2TeDSvI4qV14YfgaMt_ADSui6r3cC866h0c2skkW97QMk7D4iYho4uqGtSC3lajOhAV1ReXX2dpbPo7E-tIQhlxF6ONyz3cXbaqxE2tYwj0eChh8Aq9-njGq9GkdGrh_nvcUq24byiPYdw8u29JL75-JT972IoM-jQ2IuTrPExRVlwrKA',
    'adScriptData': 'NO REGION',
    'idToken': 'eyJhbGciOiJub25lIn0.eyJzdWIiOiIxMDA5MjA4NDAyMCIsImlzcyI6Ik1JNiIsImV4cCI6MTc1NDgwMzc0MiwiaWF0IjoxNzU0Nzg5MzQyLCJhc3MiOiJNIiwic3V0IjoiUiIsImNsaSI6ImVjb20td2ViLTEuMC4wIiwicHJvIjp7ImZuIjoiWGZ1IiwiZm51IjoiWGZ1IiwiZW0iOiJtZmthZGh3dHh3QG91dGxvb2suY29tIiwicGgiOmZhbHNlLCJsZWQiOm51bGwsImx0eSI6dHJ1ZSwic3QiOiJOTyBSRUdJT04iLCJzbiI6bnVsbH19.',
    'refreshToken': 'TGT.8dd5b1911378437c8ddc7a70305440a2-m',
    'login-session': 'RJyMZFXArMJ-FpDZ6D40t4EMtsjEkd9uledpsj7d3a_evHPP7ee4DBfzAvsv1lWV',
    'ffsession': '{%22sessionHash%22:%22e8e13c60dc4091754788910164%22}',
    'egsSessionId': 'c2439485-38fb-4a79-aed8-c3b525eb96f8',
    'pxcts': '632cd447-7588-11f0-9ab8-02b5567904c2',
    'UserLocation': '33195|25.835116|-80.322690|FL|US',
    '_gcl_au': '1.1.1289249524.1754632102',
    'ci_pixmgr': 'other',
    '_pxvid': '54fffd27-7410-11f0-9bd9-a783e0e9ea1b',
    'visitorId': '019887F14366020183189E463146C0E5',
}
HEADERS = {
    'accept': 'application/json',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7,ko;q=0.6,ar;q=0.5',
    'content-type': 'application/json',
    'origin': 'https://www.target.com',
    'priority': 'u=1, i',
    'referer': 'https://www.target.com/p/gain-aroma-boost-original-scent-he-compatible-liquid-laundry-detergent/-/A-79394245',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    'x-application-name': 'web',
    'x-gyjwza5z-a': 'cJjIU3=k1IAumcLz=XW3DT_ixCDpBrm=psyVh5orr6YVrI=wme9bjlv=HR=7flvirz1qUQ9Sx1TZCz16afXtbqnsv4y=bM3WWHX2S=B6fajxWXuJ9EcqGLnschKydhkQBAVQyLXO0bq8tKcUYIZYIKIHr=rBDilRow=riECyI06DTuqDq4nuGM2D=SEGXkHwFri0MYv-kY9txphdly=iWHBolhv=0ISWoIiw6E5qQ8VDH6qtc670EGDFmXxIvsHkby7sVtmHjNAocNZBSh1RJdeqsB2ZS4vcTv9LZI7=w=ChaabmsvUWIr-9N2VBF0ovS2k2zRDOmoZuqMH9LE6i=tmqEBVk4thOXdlwTEHsuBZVHuZwIlX5jBXqMrCdjVq1UEUMhWfWQwjKB4tMcU7zrLsmFIUf896nGzAv0fZcQYsfe0rWmyT9HN60Is1SxqH9b0=N5NZEGo-BugOkSArInwu5dFYXS4tgf-bZWwvxTxjNSs=tE8GABv_vC1I53c-jcwp1Em20XrTfVc4XI7AuuOtRBKoenvrzSAoQuGMHflKMtIbBLiHgBFY-do0-sl6UjXc2gTzWWatUJOHo6yobl67Z998_AsHuIvQKr4mfqL_Vjl97vEIyeIW9245g3wfhSJCTZUpGukf50psi3Qgx5So6M=9RjX-RhLp85O8_UMj2-kd2a7psDEyClCrYcSpFk8MzS229i9oEJ_NIhhnld7jTR54LYObnsgIRshzjBT0mVs0rK8y2mRQkKJrNQzAB09MX=cJr7ULwvcKGChqH8ro_HypBc7Ysb3YlyzMNduibXyQxJzhc8r5NAeSL2IQdS-tVRp9tGW0CDdmbO6fTv4Oy85T3gxgWJ-EBA66KMitEVy-UdxZJBWg8B-1vnptkUaeOqbIA4c4tS51-DYw=r8FLxcIh5ZE2F25dJsocEMMfCqjM3ppQ7x9=Z3StEhv6QSSaRmG3xh=U=w8QwjcUaHMmpVbkVMLO2Ibz2cQ4ZX5wTfE=dIZ6C1wcOqoOGLr3h0WyN6xU-mxR_jpM_bqn8-bVhiWpfrE75iLT5c6nzKaXDdNUeBIztrfXYxsJdf6QGdRQif2i8LBYrxlACR_B1FWJ0lTbTozltoGvJm6mvvExp1po0lsOL6QMz6aJWglw5kgTC-HyxHnf44H591i9emwrYacwtuTDlO4jQSjsKVXoV=j7GvO=Vy_c35UT56beRH0WFhSVE6BZRexlLZwOKixLskBipWDRpNOu6Iwd6Wq_Mo-Ujf9AHEsU9qO0d2ZqaAkZJ-g7dT9BZeMl_2d4nafl1Lv0_U8vug7OaYKT02wtuxwWTH4UqXLwUjYq_DXGUcv=HvLX-52ZgoGoqSdtw4J84nMsM6tYsyihOIbc5wu7-0iGYvR3HksAtro2z=TIOLL-BkKDpHvUqTG7JnBzV0FAnQ2yj2pUj=vtR8_43bmWjhhRGb_c-ERde5FHjWUWcKVAdbQ46nxL9G6eWEFOYAmoG-vN2ssIf7x33y2=IQB5h8SF01VtcSmFYu=T5laKdhG=_glojWdRbOe72VzTDsC=O5OjyUTNLKA=FLbeUZnvo_vTjQDNTA6EpFZnQ63sHr6i5I3wN27UZIFd2f9m-lOZd21tx6sbESWNR=1hAwxQwGBd=vsc9ekxjYWJ2a03gBx-9AQzHfraI2d3lI2cA0Tzh9HgWCBi5xMlBOU2=6qdivmhwhwAwB_qhlZ-5vpzOahIrqwwZqdsVtJcKVLmmSlis2cS9Zmuum4p9Q5SyfpTgAHW2JMafVuOk=Mk25ZCDOvs4hmXM3=fLCvqyMxb4D1R3mAhS22xdRnjEkqfEccK=-13AECF_gQS1OuZyAWhjxLwGF5OJCiij-BRyTlcBfpuvxJdae1Ioy1yL2A7pikZgVkp4y8xj_4DRF5HZ191d1wXNkIX-a8ghnfYl73YwFwDj27MuK4URvScm5j3dS1iOt1js8NvIsfhmJT5mTMavej4eptxiaGjG5iw7kwJt1kxvLLF2Axhb3O7ovODhr_UMJ3dticTdShQKRKZd5Z2NKdffUpMgrGFQo7qaj3Q9-TWxFS1DvuEEj2S32Exav3HEpZnIaQzJfZiYi9nJ9wYWnzsjsfGxMtwU2vwYKvJMNYbha00ovqfJowZBLQ0=K_FJ2Bapj4puXn9UQ2CMSSb_g9lBtfKit5ozzOmajcFOD5RnLav2nt2R73wqQIEEgz_qlbfoyCJAoEQBUoi16dvbh9JmceWJ8-eu31mhSGI005N5bwEYz1YwQTnb8BMKfjE3qnD_HBk60EUTLXQ2qTyek6Sf26fSWR1zYjghmWl1-oBkIFvynlHStERRLigLGwxm9SmjrZ7_72Wu5us=j1U__ri5s3lCRohCnn2B=RJ4inWYs7cl1U3oLxbfWJVQQMhGQu-iSIjaga6hQj5QAIsajpqRAjRtybdcHuCDovWO3lhWaQd9iVVHexKJwIO144ftNLAc6Ccoe_8K2bmsdID8SWDcw-5a_3kjFMEXR87pEcvAu=bMkAI=ZOlZ0pp961gBVkwF7gW62lxvcCNp0pxZdhI3WAZ-7qLl=xI-mwRCb-030ERzy1pItXB1A=na7=0MwXkXBrOOu9Tkdkpe8QcfGSEn9tS8RNQRqKJprgqr13SYt61EwGKdhEpS7sHgYrVqEE6LQl2p33=GNyuAbTEAUj3k7-z_AWLqQSCbMmpsQy7lV1RYMKZZYhmjabHB7Kij26MarM81scTLjS_El54nOLVFEGYH3i7LJOoT2QriD6_w-kUG4oxe2Q=Ycm8W9t_BRZVMqgsNJeOEUlhdcVh-Ep_T3FHgQLErry2tw2He8JW77Y=RxLvMeq=b8seTCedC664uFbzM0vwj7ULFV=LCdu28YR2kNhKdn38BQDAoUk7aYpzEi9BlnWTpEUC97R7ycwRN=pVeKuMYQ7rj4EgmFcf0s4_UiJV-GIudgxqy=r=y4xZgKOd-y_O4sWrGwrNgmZ86apxJilO-MwHvn8KkpOcsJrssahv9hdXpMd-zScv=xqihyAAeWzJ4lIqaB0l_HJpC6qcokhSbza2yhaX6kMGRHDcJfe2oKY4xrbTlnmuJS8r2x53093wlCVYIRigc2_0Ihhmyne8OKqJtRaHKfs131T7G5qbGrn-7qjMLayuxAongUs5Zf-5eaTKm1IqiBhJeSdSa=UingdaZ4Ocg6sHen4Zn88El0b31GLVSLE0_J7VgGI8v=mEAfkYHNK-JZ75HGFD7DzII7NQGGBCCvapTmREFdbAJeN0-foUrUQAB9a2zmgWXzwio=1sNwW80VNOXhWqOTskzqwmsFcHnRYVCCpeEpwmthbnDzITBCepJHxWYJ0_S4vJO-mY7JcDI6LzBLW5k6_jI14QEbjoDyvrLHfRSLt1Q41FEXSuZQ8LbgLLWdWVdEwQklcxNdfb_-OTEFNA-yEhFTIpjGILU947Jcd=mR0Yjr9x-lI_z73gEYCTmSBozp9zY3ZdYrGHv88FkCQRWAoJja=1ZK1aUUyTcNApqnOx6Ms-qZvMx_wkS21YtXUmfBrwlvArMdeOLUx-RNgWgomy09dzHeRIvWelvOD0Nq5tDqlhReO1FAHtcJDYUJcKf8W6VRxiH94lRA2jGUvAp1wjIaa8Dkx6k5=n=M2iRKW8_q8KwsTeiDFS2yKuzWL3aInXFyDScapSTjXNHmmBdwYIZDmRBjHZ=sEuhgbu=RrU99Vc=JalAYFW7qK-ReCv-rDpCwkchvQDSEQcfvl2SvixsIFpJ2b4f_AqBXGAryVzZp55fnTLx5WXGq15Ln6EA7eqTDRpDtqRIpYINnXFFKrVM_7rZj8Iu8FbSvtM1MhRmHHdpdQ6GtOaOOqDdMOVHmjQGbGFlZ2ni-C-yiKvRl-J8tUrTYiKbQjdtNy=avgb90g02Osvhb=R861NRBy9GeH5jITC1_Z=rs7zAAQdxA=8_MzOt40FftpZBzhYJUgYd2=v6yIvlSlIIdMuc7omuE7wWL26HegvZV=72-yv0V5GffS-Lp3EA2oJADndxB3jv-=bVR71VkSK2SCRmb8vMGJWdBU7X0tsMSNrcxyOCK02Ya4pGey6CrHtXIeumSx=U2os0de8wMgeyUqg84_SYeAsD4NuH1f5kLMEUhhNa1jDO5Y-7G-oHZKTawxzyEhLFBtjM_2rAo_0FXYlINUmcxJBcRLHEFbAgE=d7SX1lUw8e-KkXw6GftXoUTBoeOwUgv3lQ4HXepHeXF-ShVRcEmR6qM5uD58uSIdNH=r_O2bDTTLxNi4XMMcjp-fB7Rjm=fo6Dbi3dpC5R0VH97ph-JhaMxcVXyXLESWvKIHzFg_t0Oa5CFAdAFrqgWpXk-bM3L4SvDYFa5wpWkqz9csn9byTXpLI-Tu_=zDTfCdmV43sqX5=klk5bwvnOOnoeHoB1QZSRjyBIEb_J8YeSK8Ik=x=UlEjXCC2N3kvE8drbkWfJgbppoBarTdiMtfZamq=qOvseZfFw8jLhzXINsAwni82QWrDOsHRyKMA48VuwTKYCFS3nk4dWSXXA9hcrU2h5AfwkgvKcIl1ykLyGq5Fl_b8ppL8KJSrs=Z1xkncg7Vib=zKg7ABvWejgBg-8Mwu0W25=_0qcUjbUWycVM5-MNe8MyRHR-6x5zEJDZO7hzsagaaCyA=JxjS93y4jrev0ZZQLvHtUkjv_QSZgBJXz-nWNyZ8aWh-fb8QAj5qRjBiydkTVz5Yd-tTnb5=MOc_O8gjGMMKi3dN88O=bFx5_OVgLzIy=duoWXGYCHnd_r7m8_iF8nR3KzpNuit0AIfeGQ3790ea-xQ=FTg3Vz78Ytaly6kYTovc77FWAhuuSwXLBi7bscR5ooI1lpENRqNDGA7BX6WQO7AE0Ylatb3CEZAn3ZiijWinAwWi8kRjjamHrWHoznhw3R3l7ZGCSkifQU4Kl936-bT9tH3oHfhfsy=xU2x-Ap=sbyFpmRRbtAJ9uSgrYYANsWrdx8D66AkS44bXN5m1_aZtDArIoaVaxtyo0Ckn7j=2nS3W5rVfBTlIKcB4=S0i3xuiguCzrR6UlL=oXh0ME3=sNqAsWIiBVyVHgU7l_k8dkJLY5jFwapMOY-il1oM01a5afQOdHD3DRkQ5z3I2rekkzqkn9IIyHU96C45caqXZIne99cOeJN3ECM2kedSbFQrh_DskpFhNL6Wp8oyqi80ngyx80o6zlFjgd=9lvFwyMUNB-R2q5JrW9QQ_=V=dNFbn4-UBxLBq=L=eUuO62O0Q52rpJubTKHNV9tgcMO-8n6yEyGcYR39_RohX3dkCbUbNN-z=roEJe5SI2BKpCgHDxK7=FhFlySA22ECaJhK=LTHRa7Fn-Qn9DeVfJVqQ7SVK_5xls9indxQyo=1FYtaCfZ6M=_YHn3ltNLrS75AUsx_wSfoOoBseF=5g=nwjIW5xBiuKpSe_jlSJVavGMK111o38nLH5_mU2lwkTCH_7Er1RnF=vw4GlqF7gtwT=6M7XQgrIid78fcWIoL0mBA7NtnMAqfRgnLYpzfLklnJ4dAg0mmyZUbcgJvmlKSscEENtw2ogtuWEgB=TpwHZdivK4igHnVirELiRZ_S2jxdrrVhbI5gqAHG-sJLaL4V2S_UX_jog3f4TOA4BmJZ==FA-Q10oip2o3eSN1-xo89HNsmKSJ9giZGL_8X83o2kHqdzoNQkMBFyT8nNGvJNBzjCG2sCDTs94y4pgqk5DiZCtnegWl-XT=xeEw9dV_WZ1qQKnfXRFnNyKlsrDM8eksAQ6GiCeG7jwQuQVpEMYJyw-4g9JpWKgafizUZGr0Us6cZtax=Y6iNJQ6ZEGv6FyRmx4G1vop7-Kcrb8RcZETGX7djsk6E8DiaIboOweu3Hym4Y5zqa_a_p7UwAkKoq11w6f9Af9EG=gopDi7aa2HTiC0_9DnSvpbCLVTfyL1an0cSaME7dIXSZoHfMvnk_xnSFNSMDdOewp8jl-EvQkeW30oDyquZjrlVA3=C2giZCc-2W-cNXanm2Q_V5Gm3DCov_Wf3gOj1d_ZwRNW_K8tF=MA0KixZq=ns13t4_dTZMf-qICsiFOz2tjBlf1Zs6SRKCSJFeQs6fDmS8g9B_ConFH7pnRgT=ScrZ3LJj3VYdtG0cD4bWLy2eTJ1p7R7BixDBDFEOX67vRoKRWvbIMBBcVQg4ijbU0DWXncgwwAN_o41ZedZaNYH1E-xbC0QIKdldyogVesRrk_Nt=fRwOvUf51ZcqhSjCveIofKbJFZJK9vLCaC5AF-g-u1KxfWybigN4yoQ3mdGepGlqZjiM_084ruyxNYassdBCkUBaOE=1rEedSObCGQlwzyvrshf0m5TKxLL8VveltYGx5MSCrGHMxZHv-2Uaqc15rUb9oc4MpEvaIRgKy3E0nlifHFgfygblcG4nBjA6fZt8-JWlE6MCUeyQ9hMVxi-Rj43jZgTWIXzVMeDqSfjWrDtzsW=J5EfukD09hbNVniodH0ZNd4YpYu4N6LYbj8WcWh9UBLO_YHiXOU9O37OsAMpuInFlkz6eXkkkioCcRDiRK=q8Jug5fT2nhoijugFIfXRjrJBCTDJ0zA1J6fh4=FdEmuQ0zAdNDntlHexGB9aSRcKB_IBa12lihEYumJ9kS2u1=-dVs--9TWB6gO-z1qikcfRYGlaOzr_SgehV2zvW09kmE4XQBaQcvo7Zfj3RpvDUMOraJEsSFi7UZJHd3gBn50-USoUgZ542YBycSgUkjZjD0QMWEEgh98eGpSbj60tJs96LUTak4ihclLIq=E1sjAtaR3iF5G=Apl60oCwIx=Lh9h7AVNp241L6obNw_oacOcdRUNYm4DO84EzCJX7GR0_4awFZIQvG3-Be_HKiz3w-S4mz4-GN4uWdd8y98zWWEh9ogF_QZX9p-5oRk9553u0j5ZWNGljiHzZK45HAq4Jj8VSicwzpe6AYnvaqgGAI54W5mage6z4ap_Ee4cnh_AcgZBF1nTiTmli65TAL-JWq5Tm4Lcf_=Bv_iMFMkasC7wx1X6AxHOdHBj6uqVquDVq2=tmLoGMzBg-MDxAI06H278rXYo-H16jSFMeo=18wChBaIb3Jf2IKq3hHDQkNjy0FSmI2mU9=0r7C8e1VX=d=065Tv_T6NZJdnsqu61L=l3gtwIQj=8NYYsNp9bwqdwS52k=O6jEy9NWwY5uAipLJGS2x1iXOGtpwGub8ndTCfYFo6GewIisfBZ8z=seliRXta40djAL9ZbwuL904GNaK0-nhjay-enalwXoDhb38Fbgh0BC0FkqIwtaS8yHeO12lJY_EbQvLGTr3wT5fph5AeUo92361mQrgdmhuil3n2z402StAbC5Ks852NjUnRCBAsbM-EEmFhf8YifSrBrABF=_nTQnJYzRR3JuGSDLRHqyyInKgW5EFE2L7OY077g0k9r7-D8Ek6fqsKZ=LdZE-XMEUNBxTwJFWbXiDc6=46euXUGoRZNiiAem9yLMGjwoBDGXbOElM=SQ6Xtt5XChQkJbTx7zWQwgx9tCUeSrfxtaJA5AzGeC7JOro727s9p71pVH4bvomyUCg35J=W9CG0fS1Xg47FYAXpw3_S4hL0V4kMdbZZWIAJCdFkF0AuM9kfR81C5M_cWKKoHxij4LE4EFURRFNLDJ8Sm-Bb7scfp2h=x0g0GmvxtZdrjSSze85K_I1LlYNmin6fqZLL65q-TD4xFYFt13j7vY8ZWFnmKurw=FtUI5zHusf6E9-EAYJsmGVe4GDNaAJtBXyrb3nlVHBeIn7kDhsRWEaySjktLa3ElTENx6kUG3Ucjkb9oXxd47zck6egz7xwF7oj_jHTbGTtfx=7ElS=LwAq5FULdugEM6OOzdUDfXFZFQcCeJzruEJ3CaCB',
    'x-gyjwza5z-a0': 'FDn7nJd_UiNevhjeXJ6nGuBxN5uvxynoVICzd4U52gEhXKqLzo2ywbjLnpg_JC67-UkM8ecw4QV36naNORAITch9nTJ9XCldX5ofDw6w=cWfKEoYrsCFWeYhsqABVjWQ_E3v2242gJWJyCveGYKBuc9_R4EjWmWsfpTmmRa4_NyaX0O=T71Os3ETu7FlijuNIhnCy2XKkoG9pU1IopJhUDSvCeOtE8_O_o0tiC=DfOLCUY-2T1RlLX73yL-T6SLYJ4a_B88o9t0sZLMDZD_VTgnS6-qj2C3pxhsIA9iYYmcw0kiSWbgbi3BXOITTmNuF-6XWEmMWBeR72wbOOw4',
    'x-gyjwza5z-b': '-qkwgwk',
    'x-gyjwza5z-c': 'AADq1YeYAQAA639FLIZevGFFz4cX6MbQTQdd51KeOZxQbbVljYAil_UFcKGW',
    'x-gyjwza5z-d': 'ADaAhIDBCKGBgQGAAYIQgISigaIAwBGAzPpCxg_33ocx3sD_CACAIpf1BXChlv____-rfsZ2AQw084Gok5aY2Ged1A6lKt4',
    'x-gyjwza5z-f': 'A61Q4oeYAQAAC3vwihuEd6hPjUKaj8XLXGJngLCVdhHWe7dxeJX7nZrTEFL2ASvzwGGucr_owH9eCOfvosJeCA==',
    'x-gyjwza5z-z': 'q',
    # 'cookie': 'adScriptData=13; TealeafAkaSid=zuOKsVnb8ls_aDohVTJSwyqF4ZQXo1GE; sapphire=1; visitorId=019883BC0C7E02019F233F135AA87725; UserLocation=52404|41.9831|-91.6686|IA|US; 3YCzT93n=A4xGvIOYAQAAEBztoVDzymzpR7Ie4JthnOTCoCX7F1CUTKgGbmlhmF0anIWHAWeXrBmuchxGwH9eCOfvosJeCA|1|1|24f08bf35cb3186377fa0abf1f0071471ca59508; fiatsCookie=DSI_1771|DSN_Cedar%20Rapids%20South|DSZ_52404; ci_pixmgr=other; idToken=eyJhbGciOiJub25lIn0.eyJzdWIiOiJkNTQzYzUzZi1iNmZmLTQ4YTMtOWZhMy1iMDU3Y2NlNjk4MzMiLCJpc3MiOiJNSTYiLCJleHAiOjE3NTQ3MDk5MDcsImlhdCI6MTc1NDYyMzUwNywiYXNzIjoiTCIsInN1dCI6IkciLCJjbGkiOiJlY29tLXdlYi0xLjAuMCIsInBybyI6eyJmbiI6bnVsbCwiZm51IjpudWxsLCJlbSI6bnVsbCwicGgiOmZhbHNlLCJsZWQiOm51bGwsImx0eSI6ZmFsc2UsInN0IjoiMTMiLCJzbiI6bnVsbH19.; refreshToken=TrxkiFB8Js_Q-XSsSBvynKbXcJayikE1mOMmgHAWkKoialcnONKwwhLCavcEuLqWWMdiYzkSdjJ_MMQcgGa7VA; accessToken=eyJraWQiOiJlYXMyIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiJkNTQzYzUzZi1iNmZmLTQ4YTMtOWZhMy1iMDU3Y2NlNjk4MzMiLCJpc3MiOiJNSTYiLCJleHAiOjE3NTQ3MDk5MDcsImlhdCI6MTc1NDYyMzUwNywianRpIjoiVEdULjgzMDQ3MDkwYjkwMzQxMGI5NDJkYjBmZWNhY2VlZGMwLWwiLCJza3kiOiJlYXMyIiwic3V0IjoiRyIsImRpZCI6ImJmM2I2ZWUzYjZiNmFkYTcwMWRmMDFiNDJlZDBhMTE0ZDlkMWUyZGY2MjM5NGI1NGQxODVhZjkzY2IwOTA0MDMiLCJzY28iOiJlY29tLm5vbmUsb3BlbmlkIiwiY2xpIjoiZWNvbS13ZWItMS4wLjAiLCJhc2wiOiJMIn0.PLnySpeWms5Ysopiaq5WhX63b-zpGA-WQ_e85p0VRz8SCP3n2GQJW1yD4Z2PXGawOo1CSm3G5okldS5tfuc77GD9leUd0XflRQxMaUQlsW6yCwL9D-sfweFIAVBCOmLAZOnmgLI1zHhS6Iz493GNHp5e3EtB_1YvukWZXQvRw6jC8mcrJ3Vto4zWa1MPY1b_V6GigkHaUQy1rgF8V7hFd5BRoUqUny_nTnLjwhuZXaEbz91aougfpejm5yd885SQmnXTtcVEmu0La2duuq910_ImPKXkNXo9ZtV50I_-EOjwAY2h8T0aBTmfBoa1uSbOGG6Nm5dTZ_zSHjPLzYMAkg; pxcts=4919a452-7407-11f0-b547-862445493694; ffsession={%22sessionHash%22:%2217bd2806b482d81754623509668%22}; crl8.fpcuid=d217b2b4-96ab-4b0c-bb1d-6026583d159f; sddStore=DSI_1771|DSN_Cedar%20Rapids%20South|DSZ_52404; _px3=320b6598326ed4166b26d1f1c6cc3e44dd3ad208467fa283923c95ffdda67c98:5DpRBjkFNddFbg8OE9HzsQx35bybmbfUKv5jhW+cTjsL7U1VaY/krc6m5mWMeYTgBm/YZQMBTlU8mTz7xbMu6g==:1000:fXLsDTf/0FNEXxGYOPrPnocp3vuZdodrZwOe1eYqkduv5P19TodXQj5ADBvo1m6ZCDixuK6kRffFC6P4hcU3pWTSIHQsGl2A5kgWQMcVff0HVD7rZMWwiQS3WbQIQic4aOCzfcjxEiIO2OyQAPOSBc6idLnYmCQsVg+LlzamzfVr6QMnAMD2tc4SFy1U44+rzHQZ4C81m6wX/XD5+3OhZPKveFN6Hra3pkIMy1dC2rI=',
}

API_ENDPOINT = 'https://carts.target.com/web_checkouts/v1/cart_items'
API_PARAMS = {
    'field_groups': 'CART,CART_ITEMS,SUMMARY',
    'key': '9f36aeafbe60771e321a7cc95a78140772ab3e96'
}


# ======================== 核心函数 ========================
def extract_products_from_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    从Excel数据中提取商品信息
    返回添加了商品字典列的DataFrame
    """
    df["商品字典"] = None
    df["加购商品字典"] = None

    for idx, row in df.iterrows():
        product_dict = {}
        cart_dict = {}

        for col_name in df.columns:
            if not isinstance(col_name, str):
                continue

            link = str(row[col_name]).strip()
            if link and link.lower() != "nan":
                # 提取商品ID
                match = re.search(r"A-(\d+)", link)
                if match:
                    asin = match.group(1)

                    # 区分普通商品和加购商品
                    if "加购" in col_name:
                        qty_col = next((c for c in df.columns if "加购商品数量" in str(c)), None)
                        qty = int(row[qty_col]) if qty_col and pd.notna(row[qty_col]) else 0
                        cart_dict[asin] = qty
                    elif "链接" in col_name:
                        qty_col = col_name.replace("链接", "数量")
                        qty = int(row[qty_col]) if qty_col in df.columns and pd.notna(row[qty_col]) else 1
                        product_dict[asin] = qty

        df.at[idx, "商品字典"] = json.dumps(product_dict, ensure_ascii=False)
        df.at[idx, "加购商品字典"] = json.dumps(cart_dict, ensure_ascii=False)

    return df




def login_to_target(page,context):
    """执行登录流程"""
    page.goto("https://www.target.com/cart", wait_until="networkidle")
    page.locator('[data-test="@web/AccountLink"]').click()
    page.locator('[data-test="accountNav-signIn"]').click()
    page.wait_for_selector("#username", state="visible", timeout=5000)
    playwright_cookies = [
        {"name": name, "value": value, "domain": ".target.com", "path": "/"}
        for name, value in COOKIES.items()
    ]
    context.add_cookies(playwright_cookies)
    # # 检查是否已自动登录
    # if page.locator("#username").is_visible():
    #     print("需要手动登录")
    #     page.pause()  # 暂停用于手动登录
    # else:
    #     print("已通过cookies自动登录")

# 删除购物车
def clear_shopping_cart(page, max_retries=3):
    page.goto("https://www.target.com/cart?", wait_until="networkidle")

    for attempt in range(max_retries):
        try:
            # 每次循环都重新获取按钮
            delete_buttons = page.locator('[data-test="cartItem-deleteBtn"]')
            count = delete_buttons.count()

            if count == 0:
                print("购物车已空")
                return True

            print(f"尝试 {attempt + 1}: 找到 {count} 个商品")

            # 从后往前删除
            for i in range(count - 1, -1, -1):
                btn = page.locator('[data-test="cartItem-deleteBtn"]').nth(i)
                btn.wait_for(state="visible", timeout=5000)
                btn.scroll_into_view_if_needed()
                btn.click(timeout=10000)
                page.wait_for_timeout(1000)

            # 验证是否清空
            if page.locator('[data-test="cartItem-deleteBtn"]').count() == 0:
                print("购物车清空成功")
                return True

        except Exception as e:
            print(f"尝试 {attempt + 1} 失败: {str(e)}，刷新页面重试")
            page.reload(wait_until="networkidle")
            continue  # 刷新后继续重试

    print("多次尝试后仍未清空购物车")
    return False


def post_with_retries(url: str, params: Dict, cookies: Dict,
                      headers: Dict, json_data: Dict,
                      retries: int = 3, delay: float = 2) -> Optional[requests.Response]:
    """
    带重试机制的POST请求
    """
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(
                url,
                params=params,
                cookies=cookies,
                headers=headers,
                json=json_data,
                timeout=10
            )

            if response.status_code // 100 == 2:
                print(f"请求成功 (尝试 {attempt}): 状态码 {response.status_code}")
                return response
            else:
                print(f"请求失败 (尝试 {attempt}): 状态码 {response.status_code}")
                print(f"响应内容: {response.text[:200]}...")
        except Exception as e:
            print(f"请求异常 (尝试 {attempt}): {str(e)}")

        if attempt < retries:
            time.sleep(delay)

    return None


def add_items_to_cart(items_dict: Dict[str, int],
                      cookies: Dict,
                      headers: Dict) -> bool:
    """批量添加商品到购物车"""
    time.sleep(3)
    if not items_dict:
        return True

    for sku, qty in items_dict.items():
        if qty <= 0:
            continue

        print(f"正在添加: SKU {sku}, 数量 {qty}")
        payload = {
            'cart_item': {
                'item_channel_id': '10',
                'tcin': sku,
                'quantity': qty,
            },
            'cart_type': 'REGULAR',
            'channel_id': '10',
            'shopping_context': 'DIGITAL',
        }

        response = post_with_retries(
            API_ENDPOINT,
            params=API_PARAMS,
            cookies=cookies,
            headers=headers,
            json_data=payload
        )

        if not response:
            print(f"添加商品 {sku} 失败")
            return False

    return True

@retry(exceptions=RequestException, tries=3, delay=3)
def get_dmc_offer_ids(cookies: Dict, headers: Dict):
    """
    获取当前可用的DMC优惠券offer_id列表
    返回: 包含所有offer_id的列表，出错时返回空列表
    """
    # API请求参数
    params = {
        'cart_type': 'REGULAR',
        'field_groups': 'ADDRESSES,CART,CART_ITEMS,FINANCE_PROVIDERS,PROMOTION_CODES,SUMMARY',
        'key': 'e59ce3b531b2c39afb2e2b8a71ff10113aac2a14',
    }

    json_data = {
        'cart_type': 'REGULAR',
        'shopping_context': 'DIGITAL',
        'channel_id': '10',
        'guest_location': {
            'country': 'NZ',
            'latitude': '-41.290',
            'longitude': '174.780',
            'state': 'WGN',
            'zip_code': '06140',
        },
        'shopping_location_id': '3333',
    }

    try:
        # 发送API请求
        response = requests.put(
            'https://carts.target.com/web_checkouts/v1/cart',
            params=params,
            cookies=cookies,
            headers=headers,
            json=json_data,
            timeout=10
        )
        response.raise_for_status()  # 检查HTTP错误

        # 解析响应数据
        data = response.json()
        offer_ids = []

        if "available_offers" in data and "dmc_offers" in data["available_offers"]:
            for offer in data["available_offers"]["dmc_offers"]:
                if "offer_id" in offer:
                    offer_ids.append(offer["offer_id"])

        print(f"成功获取 {len(offer_ids)} 个DMC优惠券")
        return offer_ids

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {str(e)}")
        raise


@retry(exceptions=RequestException, tries=3, delay=3)
def apply_coupon(offer_id: str, cookies: Dict, headers: Dict) -> bool:
    """
    应用指定优惠券到购物车
    :param offer_id: 优惠券ID (如: '8889723')
    :param cookies: 当前会话cookies
    :param headers: 请求headers
    :return: 是否应用成功
    """
    # 基础请求参数
    params = {
        'cart_type': 'REGULAR',
        'field_groups': 'ADDRESSES,CART,CART_ITEMS,FINANCE_PROVIDERS,PROMOTION_CODES,SUMMARY',
        'key': 'e59ce3b531b2c39afb2e2b8a71ff10113aac2a14',
    }

    # 优惠券数据
    json_data = {
        'offer_id': str(offer_id),  # 确保转为字符串
        'offer_type': 'DMC',
    }

    try:
        # 发送请求
        response = requests.post(
            'https://carts.target.com/web_checkouts/v1/apply_offer',
            params=params,
            cookies=cookies,
            headers=headers,
            json=json_data,
            timeout=10
        )

        # 检查响应状态
        if response.status_code == 200:
            print(f"✅ 优惠券 {offer_id} 应用成功")
            return True
        return False

    except requests.exceptions.RequestException as e:
        print(f"🚨 网络请求异常: {str(e)}")
        raise  # 触发重试


@retry(exceptions=RequestException, tries=3, delay=2)
def is_free_shipping(cookies: Dict, headers: Dict) -> Optional[bool]:
    """
    判断是否免运费（自动重试3次）

    :param cookies: 会话cookies
    :param headers: 请求headers
    :return:
        - True: 免运费
        - False: 需要支付运费
        - None: 获取运费信息失败
    """
    params = {
        'cart_type': 'REGULAR',
        'field_groups': 'ADDRESSES,CART,CART_ITEMS,PAYMENT_INSTRUCTIONS,PROMOTION_CODES,SUMMARY',
        'key': 'e59ce3b531b2c39afb2e2b8a71ff10113aac2a14',
        'refresh': 'true',
    }

    try:
        response = requests.get(
            'https://carts.target.com/web_checkouts/v1/cart_views',
            params=params,
            cookies=cookies,
            headers=headers,
            timeout=8
        )
        response.raise_for_status()

        data = response.json()

        # 直接计算最终运费是否等于0
        original = sum(
            item["fulfillment"]["price"]["shipping_price"]
            for item in data["cart_items"]
        )
        discount = data["summary"]["total_shipping_discount"]
        return (original - discount) == 0

    except (KeyError, ValueError) as e:
        print(f"数据解析错误: {str(e)}")
        raise RequestException("响应数据格式异常")
    except Exception as e:
        print(f"未知错误: {str(e)}")
        raise RequestException(str(e))
# 填写地址
def order_from(row,page):
    page.goto("https://www.target.com/cart")
    time.sleep(5)
    page.locator("[data-test=\"checkout-button\"]").click()
    time.sleep(5)
    # 填写地址
    # 编辑 shipping
    edit = page.locator('[data-test="edit-shipping-button"]')
    if edit.is_visible() and not edit.is_disabled():
        edit.click()

    time.sleep(10)

    # 编辑地址
    edit = page.locator('[data-test="editButton"]').first
    if edit.is_visible() and not edit.is_disabled():
        edit.click()

    # 填写收件信息
    page.get_by_role("textbox", name="First name").fill(row["收件人名称"])
    page.get_by_role("textbox", name="Last name").fill(row["收件人名称"])
    page.locator('[data-test="@web/TypeaheadInput/Input"]').fill(row["收件人地址"])
    page.get_by_role("textbox", name="Zip code").fill(row["收件邮编"])
    page.get_by_role("textbox", name="City").fill(row["收件城市"])
    page.get_by_role("textbox", name="Phone number").fill(str(row["收件人电话"]))
    page.get_by_label("State").select_option(row["收件州省"])

    # 保存并继续 - 优先 data-test，其次文本
    save_btn = page.get_by_role("button", name="Save & continue")
    if save_btn.is_visible() and not save_btn.is_disabled():
        save_btn.click()
    else:
        save_btn = page.locator('[data-test="save_and_continue_button_step_SHIPPING"]')
        if save_btn.is_visible() and not save_btn.is_disabled():
            save_btn.click()

    time.sleep(20)

def add_giftcards(page):
    page.goto("https://www.target.com/account/giftcards")
    time.sleep(5)
    page.locator("[data-test=\"addNewButton-giftcards\"]").click()
    page.get_by_role("textbox", name="Card number").fill("044500063487893")
    page.get_by_role("textbox", name="Access number").fill("57315148")
    page.locator("[data-test=\"giftCardSubmitButton\"]").click()




# ======================== 主流程 ========================
def process_orders(df: pd.DataFrame):
    """处理所有订单的主流程"""
    # 初始化结果列
    if '操作结果' not in df.columns:
        df['操作结果'] = ""
    df['操作结果'] = df['操作结果'].astype(str)

    with sync_playwright() as playwright:
        # 初始化浏览器
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        try:
            # 登录
            login_to_target(page,context)
            add_giftcards(page)
            # 处理每一行订单
            for idx, row in df.iterrows():
                print(f"\n正在处理第 {idx + 1}/{len(df)} 行订单...")

                # 解析商品信息
                try:
                    items_dict = json.loads(row["商品字典"])
                    items_dict2 = json.loads(row["加购商品字典"])
                except Exception as e:
                    print(f"解析商品信息失败: {str(e)}")
                    df.at[idx, '操作结果'] = '数据解析失败'
                    continue

                # 清空购物车
                if not clear_shopping_cart(page):
                    df.at[idx, '操作结果'] = '清空购物车失败'
                    continue

                # 添加商品
                success1 = add_items_to_cart(items_dict, COOKIES, HEADERS)
                success2 = add_items_to_cart(items_dict2, COOKIES, HEADERS)

                # 查看优惠卷
                dmc = get_dmc_offer_ids(COOKIES, HEADERS)
                if not dmc:
                    df.at[idx, '操作结果'] = '没有购物劵'
                    continue
                for i in dmc:
                    apply_coupon(i, COOKIES, HEADERS)
                if not is_free_shipping(COOKIES, HEADERS):
                    df.at[idx, '操作结果'] = '存在运费'
                    continue
                order_from(row, page)
                time.sleep(20)
                # 记录结果
                if success1 and success2:
                    df.at[idx, '操作结果'] = '采购成功'
                    print("当前订单处理成功")
                else:
                    df.at[idx, '操作结果'] = '采购失败'
                    print("当前订单处理失败")

                # 间隔等待
                page.wait_for_timeout(3000)

        finally:
            # 确保浏览器关闭
            browser.close()

    return df


# ======================== 执行入口 ========================
if __name__ == "__main__":
    try:
        # 读取数据
        input_file = '订单数据.xlsx'
        output_file = '订单处理结果.xlsx'

        print(f"正在读取Excel文件: {input_file}")
        df = pd.read_excel(input_file)

        # 提取商品信息
        print("正在解析商品信息...")
        df = extract_products_from_excel(df)

        # 处理订单
        print("开始处理订单...")
        processed_df = process_orders(df)

        # 保存结果
        print(f"保存结果到: {output_file}")
        processed_df.to_excel(output_file, index=False)

        print("所有订单处理完成！")
        print("操作结果统计:")
        print(processed_df['操作结果'].value_counts())

    except Exception as e:
        print(f"程序运行出错: {str(e)}")
        raise