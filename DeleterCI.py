# coding=utf-8

import logger
import netutils

from appilog.common.system.types.vectors import ObjectStateHolderVector
from appilog.common.system.types import ObjectStateHolder

from com.hp.ucmdb.discovery.library.credentials.dictionary import ProtocolDictionaryManager
from com.hp.ucmdb.api import UcmdbServiceFactory, UcmdbServiceProvider, UcmdbServiceProvider
from com.hp.ucmdb.api.topology import TopologyUpdateService, TopologyModificationData, ModifyMode

from com.hp.ucmdb.api.topology import QueryDefinition
from com.hp.ucmdb.discovery.library.clients import ClientsConsts


def DiscoveryMain(Framework):

    # Создание подключения
    def createUcmdbService(Framework):
        # Получение протоколов
        protocolNames = netutils.getAvailableProtocols(Framework, ClientsConsts.HTTP_PROTOCOL_NAME, "Тут указывается нужный ip")

        ucmdbService = None

        # Подключение к протоколу и получение данных
        if not protocolNames:
            logger.debug("Нет доступных протоколов в HTTP или учетные данные не найдены")
            return None
        for protocolName in protocolNames:
            protocol = ProtocolDictionaryManager.getProtocolById(protocolName)
            if not protocol:
                continue

            HOST = protocol.getProtocolAttribute('host')
            PROTOCOL = protocol.getProtocolAttribute('protocol')
            PORT = int(protocol.getProtocolAttribute('protocol_port'), 10)
            USERNAME = protocol.getProtocolAttribute('protocol_username')
            PASSWORD = protocol.getProtocolAttribute('protocol_password')

            # Создание соединение
            try:
                serviceProvider = UcmdbServiceFactory.getServiceProvider(PROTOCOL, HOST, PORT)
                clientContext = serviceProvider.createClientContext("Main")
                credentials = serviceProvider.createCredentials(USERNAME, PASSWORD)
                ucmdbService = serviceProvider.connect(credentials, clientContext)
                break
            except:
                logger.debug("Credentials {0} doesn't match!".format(protocolName))
        return ucmdbService


    ucmdbService = createUcmdbService(Framework)
    queryService = ucmdbService.getTopologyQueryService()
    queryFactory = queryService.getFactory()

    topologyUpdateService = ucmdbService.getTopologyUpdateService()
    topologyUpdateFactory = topologyUpdateService.getFactory()
    topologyCleanupData = topologyUpdateFactory.createTopologyModificationData();

    try:
        def getLinksByIDs(queryFactory, queryService):
            queryDefinition = queryFactory.createQueryDefinition("Create Links")

            # Создание образа КЕ и связи
            hostNode = queryDefinition.addNode("Node").ofType("node").queryProperties(["global_id", "name", "root_class"])
            ipNode = queryDefinition.addNode("ip").ofType("ip_address").queryProperty("ip_address")
            # Связываем их
            hostNode.linkedTo(ipNode).withLinkOfType("containment").atLeast(1)

            # Создаем query
            topology = queryService.executeQuery(queryDefinition)

            # Получаем на основе созданной квери КЕ'шки
            nodes = topology.getCIsByName("Node")

            # Ограничитель удаления КЕ
            count = 0

            for nodeCI in nodes:
                try:
                    # Вывод в консоль полученных КЕ
                    logger.debug('----------------NODE--------------')
                    logger.debug("Node " + nodeCI.getPropertyValue("global_id") + "  " + nodeCI.getPropertyValue("name") + "  " + nodeCI.getPropertyValue("root_class"))

                    logger.debug('----------------IP--------------')
                    for relation in nodeCI.getOutgoingRelations():
                        logger.debug(relation.getEnd2CI().getPropertyValue("ip_address") + "  ")

                    # Удаление КЕ
                    host = topologyCleanupData.addCI("Node")
                    host.setPropertyValue("global_id", nodeCI.getPropertyValue("global_id"))
                    topologyModification = topologyUpdateFactory.createTopologyModification()
                    topologyModification.setDataForDelete(topologyCleanupData)

                    count += 1
                    if count >= 1000:
                        break
                except:
                    continue

            topologyUpdateService.execute(topologyModification, ModifyMode.OPTIMISTIC)

    except Exception, e:
        logger.debug('Warning-------------------!',e)

    r = getLinksByIDs(queryFactory, queryService)
    logger.debug(r)

    return ObjectStateHolderVector()
