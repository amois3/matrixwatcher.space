#!/usr/bin/env python3
"""
Oracle Cloud Instance Auto-Creator
Автоматически пытается создать VM.Standard.A1.Flex инстанс каждые 5 минут

🚀 ЛАЙФХАК 2026: Upgrade to PAYG для приоритета!
   Это НЕ означает платить - ты остаёшься в Always Free
   НО получаешь приоритет при создании инстансов
   Результат: инстанс создастся за часы вместо дней
"""

import oci
import time
import sys
from datetime import datetime

# Конфигурация
CONFIG_FILE = "~/.oci/config"
CONFIG_PROFILE = "DEFAULT"

# Параметры инстанса
COMPARTMENT_ID = "ocid1.tenancy.oc1..aaaaaaaaekvv4bwk33z6xri6prhvmlumhbytuwy75iv5illlukvpctccy6da"
AVAILABILITY_DOMAINS = ["AD-1", "AD-2", "AD-3"]  # Попробуем все 3
SHAPE = "VM.Standard.A1.Flex"
OCPUS = 1
MEMORY_GB = 6
IMAGE_ID = None  # Будет получен автоматически
DISPLAY_NAME = "matrix-watcher-instance"

# VCN параметры (если создаём новую сеть)
VCN_CIDR = "10.0.0.0/16"
SUBNET_CIDR = "10.0.0.0/24"

RETRY_INTERVAL = 120  # 2 минуты - безопасный интервал


def log(message):
    """Логирование с временной меткой"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def wait_with_backoff(attempt):
    """Экспоненциальная задержка при ошибках"""
    # После каждой неудачной попытки ждём дольше
    base_wait = RETRY_INTERVAL
    backoff = min(base_wait * (2 ** (attempt - 1)), 3600)  # Макс 1 час
    log(f"⏳ Жду {backoff // 60} минут до следующей попытки (попытка #{attempt})...")
    time.sleep(backoff)


def get_ubuntu_image_id(compute_client, compartment_id):
    """Получить OCID образа Ubuntu 22.04"""
    log("Ищу образ Ubuntu 22.04...")
    
    images = compute_client.list_images(
        compartment_id=compartment_id,
        operating_system="Canonical Ubuntu",
        operating_system_version="22.04",
        shape=SHAPE,
        sort_by="TIMECREATED",
        sort_order="DESC"
    ).data
    
    if images:
        image_id = images[0].id
        log(f"Найден образ: {images[0].display_name} ({image_id})")
        return image_id
    else:
        log("❌ Образ Ubuntu 22.04 не найден!")
        return None


def get_or_create_vcn(network_client, compartment_id):
    """Получить существующую VCN или создать новую"""
    log("Проверяю VCN...")
    
    vcns = network_client.list_vcns(compartment_id=compartment_id).data
    
    if vcns:
        vcn = vcns[0]
        log(f"Использую существующую VCN: {vcn.display_name}")
        return vcn.id
    
    log("Создаю новую VCN...")
    vcn_details = oci.core.models.CreateVcnDetails(
        cidr_block=VCN_CIDR,
        compartment_id=compartment_id,
        display_name="matrix-watcher-vcn"
    )
    
    vcn = network_client.create_vcn(vcn_details).data
    log(f"VCN создана: {vcn.id}")
    
    # Создаём Internet Gateway
    log("Создаю Internet Gateway...")
    ig_details = oci.core.models.CreateInternetGatewayDetails(
        compartment_id=compartment_id,
        vcn_id=vcn.id,
        is_enabled=True,
        display_name="matrix-watcher-ig"
    )
    ig = network_client.create_internet_gateway(ig_details).data
    
    # Обновляем route table
    log("Настраиваю маршрутизацию...")
    route_tables = network_client.list_route_tables(
        compartment_id=compartment_id,
        vcn_id=vcn.id
    ).data
    
    if route_tables:
        rt = route_tables[0]
        route_rules = [
            oci.core.models.RouteRule(
                network_entity_id=ig.id,
                destination="0.0.0.0/0",
                destination_type="CIDR_BLOCK"
            )
        ]
        network_client.update_route_table(
            rt.id,
            oci.core.models.UpdateRouteTableDetails(route_rules=route_rules)
        )
    
    return vcn.id


def get_or_create_subnet(network_client, compartment_id, vcn_id, ad_name):
    """Получить существующий subnet или создать новый"""
    log("Проверяю subnet...")
    
    subnets = network_client.list_subnets(
        compartment_id=compartment_id,
        vcn_id=vcn_id
    ).data
    
    if subnets:
        subnet = subnets[0]
        log(f"Использую существующий subnet: {subnet.display_name}")
        return subnet.id
    
    log("Создаю новый subnet...")
    subnet_details = oci.core.models.CreateSubnetDetails(
        cidr_block=SUBNET_CIDR,
        compartment_id=compartment_id,
        vcn_id=vcn_id,
        display_name="matrix-watcher-subnet"
    )
    
    subnet = network_client.create_subnet(subnet_details).data
    log(f"Subnet создан: {subnet.id}")
    return subnet.id


def create_instance(compute_client, compartment_id, ad_name, subnet_id, image_id):
    """Попытка создать инстанс"""
    log(f"Пытаюсь создать инстанс в {ad_name}...")
    
    instance_details = oci.core.models.LaunchInstanceDetails(
        availability_domain=ad_name,
        compartment_id=compartment_id,
        shape=SHAPE,
        display_name=DISPLAY_NAME,
        source_details=oci.core.models.InstanceSourceViaImageDetails(
            image_id=image_id,
            source_type="image"
        ),
        create_vnic_details=oci.core.models.CreateVnicDetails(
            subnet_id=subnet_id,
            assign_public_ip=True
        ),
        shape_config=oci.core.models.LaunchInstanceShapeConfigDetails(
            ocpus=OCPUS,
            memory_in_gbs=MEMORY_GB
        ),
        metadata={
            "ssh_authorized_keys": open("/Users/amois/Downloads/amoisejevs3@gmail.com-2026-01-19T11_45_18.939Z.pem.pub", "r").read().strip()
        }
    )
    
    try:
        response = compute_client.launch_instance(instance_details)
        instance = response.data
        log(f"✅ УСПЕХ! Инстанс создан: {instance.id}")
        log(f"Статус: {instance.lifecycle_state}")
        return instance
    
    except oci.exceptions.ServiceError as e:
        if e.status == 429:
            # Rate limiting - ждём дольше
            log(f"⚠️  {ad_name}: Rate limit (429)! Ждём 5 минут...")
            time.sleep(300)  # 5 минут
            return None
        elif "Out of capacity" in str(e) or "Out of host capacity" in str(e):
            log(f"❌ {ad_name}: Нет capacity (ждём освобождения)")
            return None
        else:
            log(f"❌ {ad_name}: {e.message if hasattr(e, 'message') else e}")
            return None
    except Exception as e:
        # Обработка ошибок соединения и других ошибок
        log(f"⚠️  Ошибка соединения: {type(e).__name__}")
        return None


def wait_for_instance_running(compute_client, instance_id):
    """Ждём пока инстанс запустится"""
    log("Жду запуска инстанса...")
    
    while True:
        instance = compute_client.get_instance(instance_id).data
        state = instance.lifecycle_state
        log(f"Статус: {state}")
        
        if state == "RUNNING":
            log("✅ Инстанс запущен!")
            return instance
        elif state in ["TERMINATED", "TERMINATING"]:
            log("❌ Инстанс завершён")
            return None
        
        time.sleep(10)


def get_public_ip(network_client, instance_id):
    """Получить публичный IP инстанса"""
    log("Получаю публичный IP...")
    
    vnics = network_client.list_vnic_attachments(
        instance_id=instance_id
    ).data
    
    if vnics:
        vnic_id = vnics[0].vnic_id
        vnic = network_client.get_vnic(vnic_id).data
        public_ip = vnic.public_ip
        log(f"✅ Публичный IP: {public_ip}")
        return public_ip
    
    return None


def main():
    """Основной цикл"""
    log("🚀 Запуск Oracle Instance Auto-Creator")
    log(f"Буду пытаться создать инстанс каждые {RETRY_INTERVAL // 60} минут")
    log("Нажми Ctrl+C для остановки")
    log("")
    log("💡 ЛАЙФХАК 2026:")
    log("   Если ты в Always Free и долго ждёшь - UPGRADE TO PAYG!")
    log("   • Зайди в Billing → Upgrade and Manage Payment")
    log("   • Нажми 'Upgrade your account' под Pay As You Go")
    log("   • Это НЕ означает платить (остаёшься в Always Free)")
    log("   • НО получаешь ПРИОРИТЕТ при создании инстансов")
    log("   • Результат: инстанс за часы вместо дней!")
    log("   • Установи Budget Alert на $1 для безопасности")
    log("")
    
    try:
        # Инициализация клиентов
        config = oci.config.from_file(CONFIG_FILE, CONFIG_PROFILE)
        compute_client = oci.core.ComputeClient(config)
        network_client = oci.core.VirtualNetworkClient(config)
        identity_client = oci.identity.IdentityClient(config)
        
        log("✅ Подключение к Oracle Cloud установлено")
        
        # Получаем availability domains
        ads = identity_client.list_availability_domains(COMPARTMENT_ID).data
        ad_names = [f"{ads[0].name[:-1]}{i}" for i in [1, 2, 3]]
        log(f"Availability domains: {', '.join(ad_names)}")
        
        # Получаем образ Ubuntu
        image_id = get_ubuntu_image_id(compute_client, COMPARTMENT_ID)
        if not image_id:
            log("❌ Не могу продолжить без образа Ubuntu")
            return
        
        # Настраиваем сеть
        vcn_id = get_or_create_vcn(network_client, COMPARTMENT_ID)
        subnet_id = get_or_create_subnet(network_client, COMPARTMENT_ID, vcn_id, ad_names[0])
        
        log("\n" + "="*60)
        log("Начинаю попытки создания инстанса...")
        log("="*60 + "\n")
        
        attempt = 0
        while True:
            attempt += 1
            log(f"\n--- Попытка #{attempt} ---")
            
            # Пробуем все availability domains
            for ad_name in ad_names:
                instance = create_instance(
                    compute_client,
                    COMPARTMENT_ID,
                    ad_name,
                    subnet_id,
                    image_id
                )
                
                if instance:
                    # Успех! Ждём запуска
                    instance = wait_for_instance_running(compute_client, instance.id)
                    if instance:
                        public_ip = get_public_ip(network_client, instance.id)
                        
                        log("\n" + "="*60)
                        log("🎉 ИНСТАНС УСПЕШНО СОЗДАН!")
                        log("="*60)
                        log(f"Instance ID: {instance.id}")
                        log(f"Public IP: {public_ip}")
                        log(f"SSH команда: ssh -i /Users/amois/Downloads/amoisejevs3@gmail.com-2026-01-19T11_45_18.939Z.pem ubuntu@{public_ip}")
                        log("="*60)
                        
                        return  # Выходим из программы
            
            # Не получилось - ждём с backoff и пробуем снова
            # После каждых 10 попыток увеличиваем интервал (меньше 429 ошибок)
            if attempt <= 10:
                wait_time = RETRY_INTERVAL  # 2 минуты
            elif attempt <= 30:
                wait_time = RETRY_INTERVAL * 2  # 4 минуты
            elif attempt <= 60:
                wait_time = RETRY_INTERVAL * 3  # 6 минут
            else:
                wait_time = RETRY_INTERVAL * 5  # 10 минут (экономим API calls)

            log(f"\n⏳ Жду {wait_time // 60} минут до следующей попытки (attempt #{attempt})...")
            time.sleep(wait_time)
    
    except KeyboardInterrupt:
        log("\n\n⛔ Остановлено пользователем")
        sys.exit(0)
    
    except Exception as e:
        log(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
