# Data Preprocessor for Tableau Dashboard of Attale Pro



## Introduction



> Project for creating a data preprocessor application running on AWS EC2 before uploading database data to S3 Bucket



생애설계 앱 서비스인 Attale Pro를 개발하고 운영하는 라이프플래닝연구소(Life Planning Lab)에서 진행한 기관 클라이언트를 대상으로 하는 대시보드 서비스 개발 프로젝트의 데이터 전처리 프로그램입니다. 





## Architecture



![preprocessor](https://cdn.jsdelivr.net/gh/Glanceyes/Image-Repository/2022/06/20/20220620_1655734286.png)



Attale Pro 어플리케이션을 이용하는 구성원의 전반적인 데이터를 기관 담당자가 손쉽게 확인할 수 있도록 Tableau 기반의 대시보드를 제작해야 했고, 이를 위해서 DB 내 데이터를 대시보드 요구사항에 맞게 전처리하여 데이터 웨어하우스에 저장하는 파이프라인을 구축했습니다. 

본 프로그램은 위 과정의 앞단에 해당되며, Attale Pro의 AWS RDS DB의 데이터를 가져와 Python 코드로 전처리하여 AWS S3 Bucket에 csv 파일로 저장하는 워크플로우를 거칩니다.





## Stack



- Python, SQL
- AWS S3, Data Pipeline, RedShift
