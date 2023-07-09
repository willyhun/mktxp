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

        monitor_labels = ['frequency', 'noise_floor', 'overall_tx_ccq', 'registered_clients', 'registered_peers']
        monitor_records = InterfaceMonitorMetricsDataSource.metric_records(router_entry, metric_labels = monitor_labels, kind = WirelessMetricsDataSource.wireless_package(router_entry))   
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

        # the client info metrics
        if router_entry.config_entry.wireless_clients:
            registration_labels = ['interface', 'ssid', 'mac_address', 'tx_rate', 'rx_rate', 'uptime', 'bytes', 'signal_to_noise', 'tx_ccq', 'signal_strength', 'signal']
            registration_records = WirelessMetricsDataSource.metric_records(router_entry, metric_labels = registration_labels)
            if registration_records:
                for registration_record in registration_records:
                    BaseOutputProcessor.augment_record(router_entry, registration_record)

                tx_byte_metrics = BaseCollector.counter_collector('wlan_clients_tx_bytes', 'Number of sent packet bytes', registration_records, 'tx_bytes', ['dhcp_name'])
                yield tx_byte_metrics

                rx_byte_metrics = BaseCollector.counter_collector('wlan_clients_rx_bytes', 'Number of received packet bytes', registration_records, 'rx_bytes', ['dhcp_name'])
                yield rx_byte_metrics

                signal_strength_metrics = BaseCollector.gauge_collector('wlan_clients_signal_strength', 'Average strength of the client signal recevied by AP', registration_records, 'signal_strength', ['dhcp_name'])
                yield signal_strength_metrics

                signal_to_noise_metrics = BaseCollector.gauge_collector('wlan_clients_signal_to_noise', 'Client devices signal to noise ratio', registration_records, 'signal_to_noise', ['dhcp_name'])
                yield signal_to_noise_metrics

                tx_ccq_metrics = BaseCollector.gauge_collector('wlan_clients_tx_ccq', 'Client Connection Quality (CCQ) for transmit', registration_records, 'tx_ccq', ['dhcp_name'])
                yield tx_ccq_metrics

                registration_metrics = BaseCollector.info_collector('wlan_clients_devices', 'Client devices info', 
                                        registration_records, ['dhcp_name', 'dhcp_address', 'rx_signal', 'ssid', 'tx_rate', 'rx_rate', 'interface', 'mac_address', 'uptime'])
                yield registration_metrics

