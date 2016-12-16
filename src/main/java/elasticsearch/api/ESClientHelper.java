package elasticsearch.api;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import solutions.siren.join.SirenJoinPlugin;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 作者: 王坤造
 * 日期: 2016/12/5 11:21
 * 名称：获取es客户端
 * 备注：至少需要2个参数:集群名称,es集群中一台节点的ip端口号
 */
public class ESClientHelper {
    //集群名称
    public static final String clusterName = "testes";
    //存储es的ip,host
    private HashMap<String, Integer> ips = new HashMap<String, Integer>() {{
        put("192.168.2.150", 9300);
    }};
    private ConcurrentHashMap<String, Client> clientMap = new ConcurrentHashMap<String, Client>();

    private ESClientHelper() {
        if (clientMap.size() < 1) {
            init();
        }
    }

    private void init() {
        //配置参数参考：http://blog.csdn.net/ljc2008110/article/details/48630609
        //Settings settings = ImmutableSettings.settingsBuilder()
        Settings setting = Settings.settingsBuilder()
                //服务器的集群名称,如果集群名称不匹配则报如下错误。
                //node null not part of the cluster Cluster [elasti], ignoring...
                //Exception in thread "main" NoNodeAvailableException[None of the configured nodes are available: [{#transport#-1}{192.168.1.128}{192.168.1.128:9300}]]
                .put("cluster.name", clusterName)
                //使客户端去嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端中，
                //这样做的好处是你不用手动设置集群里所有集群的ip到连接客户端，
                //它会自动帮你添加，并且自动发现新加入集群的机器。
                .put("client.transport.sniff", true)
                .build();
        InetSocketTransportAddress[] transportAddress = getAllAddress(ips);
        addClient(setting, getAllAddress(ips));
    }

    private void addClient(Settings setting, InetSocketTransportAddress[] transportAddress) {
        Client client = TransportClient.builder().settings(setting)
                .addPlugin(DeleteByQueryPlugin.class)//添加delete-by-query插件
                .addPlugin(SirenJoinPlugin.class)//添加siren插件
                .build()
                //上面有添加client.transport.sniff配置，所以这里添加一台机器就OK【如果没有修改端口号，则API客户端的端口号为9300，集群测试的时候用9200】
                .addTransportAddresses(transportAddress);
        clientMap.put(clusterName, client);
    }

    private InetSocketTransportAddress[] getAllAddress(HashMap<String, Integer> ips) {
        InetSocketTransportAddress[] addressList = new InetSocketTransportAddress[ips.size()];
        try {
            int i = 0;
            for (String ip : ips.keySet()) {
                addressList[i] = new InetSocketTransportAddress(InetAddress.getByName(ip), ips.get(ip));
                i++;
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return addressList;
    }


    public Client getClient() {
        return clientMap.get(clusterName);
    }

    public static final ESClientHelper getInstance() {
        return ClientHolder.INSTANCE;
    }

    private static class ClientHolder {
        private static final ESClientHelper INSTANCE = new ESClientHelper();
    }
}