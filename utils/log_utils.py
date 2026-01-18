
import logging
import os 

def setup_logger(name, log_file):
    # logger 생성 
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # 이미 핸들러가 있으면 중복 방지
    if logger.handlers:
        return logger
    
    # 출력 형식 지정
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 콘솔 출력 핸들러
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # 파일 저장 핸들러
    if log_file:
    # 로그 디렉토리 생성
        os.makedirs(os.path.dirname(log_file), exist_ok= True)

        file_handler = logging.FileHandler(log_file, encoding= 'utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

     