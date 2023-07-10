# coding=utf8


from mktxp.flow.processor.output import BaseOutputProcessor
from mktxp.collector.base_collector import BaseCollector
from mktxp.datasource.w60g_ds import W60GMetricsDataSource
from mktxp.datasource.interface_ds import InterfaceMonitorMetricsDataSource


class W60GCollector(BaseCollector):
    ''' W60G Metrics collector
    '''    
    @staticmethod
    def collect(router_entry):
        if not router_entry.config_entry.w60g:
            return

        monitor_labels = ['frequency', 'connected', 'distance', 'mac_address', 'name', 'comment', 'remote-address', 'rssi', 'signal', 'tx-mcs', 'tx-packet-error-rate', 'tx-phy-rate', 'tx-sector']
        monitor_records = InterfaceMonitorMetricsDataSource.metric_records(router_entry, metric_labels = monitor_labels, kind = W60GMetricsDataSource.interface_type(router_entry))   
        if monitor_records:
            # sanitize records for relevant labels
            noise_floor_records = [monitor_record for monitor_record in monitor_records if monitor_record.get('noise_floor')]
            tx_ccq_records = [monitor_record for monitor_record in monitor_records if monitor_record.get('overall_tx_ccq')]
            registered_clients_records  = [monitor_record for monitor_record in monitor_records if monitor_record.get('registered_clients')]

            if noise_floor_records:
                noise_floor_metrics = BaseCollector.gauge_collector('wlan_noise_floor', 'Noise floor threshold', noise_floor_records, 'noise_floor', ['channel'])
                yield noise_floor_metrics

            if tx_ccq_records:
                overall_tx_ccq_metrics = BaseCollector.gauge_collector('wlan_overall_tx_ccq', 'Client Connection Quality for transmitting', tx_ccq_records, 'overall_tx_ccq', ['channel'])
                yield overall_tx_ccq_metrics

            if registered_clients_records:
                registered_clients_metrics = BaseCollector.gauge_collector('wlan_registered_clients', 'Number of registered clients', registered_clients_records, 'registered_clients', ['channel'])
                yield registered_clients_metrics

                signal_strength_metrics = BaseCollector.gauge_collector('wlan_clients_signal_strength', 'Average strength of the client signal recevied by AP', registration_records, 'signal_strength', ['dhcp_name'])
                yield signal_strength_metrics

                signal_to_noise_metrics = BaseCollector.gauge_collector('wlan_clients_signal_to_noise', 'Client devices signal to noise ratio', registration_records, 'signal_to_noise', ['dhcp_name'])
                yield signal_to_noise_metrics

                tx_ccq_metrics = BaseCollector.gauge_collector('wlan_clients_tx_ccq', 'Client Connection Quality (CCQ) for transmit', registration_records, 'tx_ccq', ['dhcp_name'])
                yield tx_ccq_metrics

        # the client info metrics
        if router_entry.config_entry.w60g_peers:
            interface_labels = 'frequency', 'running', 'ssid', 'mac-address', 'name', 'mode', 'rx-mpdu-crc-err',  'rx-mpdu-crc-ok', 'rx-ppdu', 'tx-fw-msdu', 'tx-io-msdu', 'tx-mpdu-new', 'tx-mpdu-retry', 'tx-mpdu-total', 'tx-ppdu','tx-ppdu-from-q', 'tx-sw-msdu', 'tx-sector']
            interface_records = W60GMetricsDataSource.metric_records(router_entry, metric_labels = interface_labels)
            if interface_records:
                for interface_record in interface_records:
                    BaseOutputProcessor.augment_record(router_entry, interface_record)

                tx_ppdu_metrics = BaseCollector.counter_collector('tx-ppdu', 'Number of sent data unites', interface_records, 'tx_ppdu', ['name'])
                yield tx_ppdu_metrics

                rx_ppdu_metrics = BaseCollector.counter_collector('rx-ppdu', 'Number of received data units', interface_records, 'rx_ppdu', ['name'])
                yield rx_ppdu_metrics

                interface_metrics = BaseCollector.info_collector('w60g_imterface_metrics', 'Running Interface Metric', 
                                        interface_records, ['name', 'mac-address', 'ssid', 'tx-ppdu', 'rx-ppdu', 'running'])
                yield registration_metrics


