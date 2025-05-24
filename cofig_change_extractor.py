
import re
from dataclasses import dataclass
import sys
import functools
import datetime

min_time: datetime.datetime = datetime.datetime(2050, 12, 31, 23, 59, 59)
max_time: datetime.datetime = datetime.datetime(2000, 1, 1, 0, 0, 0)

@dataclass
class PermanentUUID:
    permanent_uuid: str

    def __str__(self):
        return self.permanent_uuid[0:4]

    def __hash__(self):
        return hash(self.permanent_uuid)

@dataclass
@functools.total_ordering
class Peer:
    permanent_uuid: PermanentUUID
    member_type: str
    host: str
    port: int
    cloud: str
    region: str
    zone: str

    def __str__(self):
        return f'{self.permanent_uuid} ({self.member_type})'

    def ToString(self, leader_uuid):
        if self.permanent_uuid == leader_uuid:
            return f'{self.permanent_uuid} (LEADER)'
        return f'{self.permanent_uuid} ({self.member_type})'

    def __eq__(self, other):
        if not isinstance(other, Peer):
            return NotImplemented
        return self.permanent_uuid == other.permanent_uuid

    def __lt__(self, other):
        if not isinstance(other, Peer):
            return NotImplemented
        return self.permanent_uuid < other.permanent_uuid

    def __hash__(self):
        return hash(self.permanent_uuid)

@dataclass
@functools.total_ordering
class ConfigChange:
    timestamp: str
    tablet: str
    reporting_peer: PermanentUUID
    term : int
    leader_uuid: PermanentUUID
    peers: list[Peer]
    old_role: str
    new_role: str
    line: str

    def __str__(self):
        return f"{self.timestamp} Term: {self.term} {self.reporting_peer}: {self.old_role} -> {self.new_role} {self.PeersToString()}"

    def PeersToString(self):
        return f"Peers: {', '.join([f'{peer.ToString(self.leader_uuid)}' for peer in self.peers])}"

    def __eq__(self, other):
        if not isinstance(other, ConfigChange):
            return NotImplemented
        return (self.term, self.leader_uuid, self.peers) == (other.term, other.leader_uuid, other.peers)

    def __lt__(self, other):
        if not isinstance(other, ConfigChange):
            return NotImplemented
        return (self.term, self.leader_uuid, self.peers) < (other.term, other.leader_uuid, other.peers)

    def Diff(self, previous_config):
        if previous_config is None:
            return self.__str__()

        # if self.leader_uuid != self.reporting_peer and previous_config.leader_uuid != self.reporting_peer:
        #     return None

        out_str = f"{self.timestamp} Term: {self.term} "
        if not isinstance(previous_config, ConfigChange):
            return NotImplemented
        if self.leader_uuid != previous_config.leader_uuid:
            if self.leader_uuid == "":
                return f"{out_str} Leader lost: {previous_config.leader_uuid}"
            if previous_config.leader_uuid == "":
                return f"{out_str} New Leader elected: {self.leader_uuid}"
        if self.peers != previous_config.peers:
            new_peers = set(self.peers) - set(previous_config.peers)
            removed_peers = set(previous_config.peers) - set(self.peers)
            if new_peers:
                new_peers_str = ', '.join([str(peer) for peer in new_peers])
                out_str += f" New peers added: {new_peers_str}. "
            if removed_peers:
                removed_peers_str = ', '.join([str(peer) for peer in removed_peers])
                out_str += f" Peers removed: {removed_peers_str}. "
            return out_str
        return None

@dataclass
class TabletHistory:
    tablet: str
    config_changes: list[ConfigChange]

def process_config_change_line(line):
    """
    Process a line from the config change file and extract relevant information.
    """

    # time_regex = r"\w(\d+) ([\w:.]+) \d+ [\w.:]+]"
    # time_match = re.match(time_regex, line)
    # if time_match:
    #     global min_time, max_time
    #     # convert 15:37:43.085095 to datetime
    #     timestamp = datetime.datetime.strptime(f"{time_match.group(1)}:{time_match.group(2)}", "%m%d:%H:%M:%S.%f")
    #     # convert 0523 to the date part of the timestamp
    #     timestamp = timestamp.replace(year=datetime.datetime.now().year)
    #     if timestamp < min_time:
    #         min_time = timestamp
    #     if timestamp > max_time:
    #         max_time = timestamp

    regex = r"\w+ ([\w:.]+) \d+ [\w.:]+] T (\w+) P (\w+): .* Consensus state: current_term: (\d+) leader_uuid: \"(|\w+)\" .*"
    match = re.match(regex, line)

    if not match:
        return None

    config = ConfigChange(
        timestamp=match.group(1),
        tablet=match.group(2),
        reporting_peer=PermanentUUID(match.group(3)),
        term= int(match.group(4)),
        leader_uuid=PermanentUUID(match.group(5)),
        peers=[],
        old_role="",
        new_role="",
        line=line
    )

    role_regex = r"Updating active role from (\w+) to (\w+)"
    role_match = re.search(role_regex, line)
    if role_match:
        config.old_role = role_match.group(1)
        config.new_role = role_match.group(2)

    for peer in line.split("peers ")[1:]:
        peer_regex = r"{ permanent_uuid: \"(\w+)\" member_type: (\w+) last_known_private_addr { host: \"([\d.]+)\" port: (\d+) } cloud_info { placement_cloud: \"(\w+)\" placement_region: \"(\w+)\" placement_zone: \"(\w+)\" } }"
        peer_match = re.match(peer_regex, peer)

        if not peer_match:
            raise ValueError(f"Peer line does not match expected format: {peer}")

        config.peers.append(Peer(
            permanent_uuid=PermanentUUID(peer_match.group(1)),
            member_type=peer_match.group(2),
            host=peer_match.group(3),
            port=int(peer_match.group(4)),
            cloud=peer_match.group(5),
            region=peer_match.group(6),
            zone=peer_match.group(7)
        ))

        config.peers.sort(key=lambda x: x.permanent_uuid.permanent_uuid)

    return config

def demo():

    configs = []
    configs.append(process_config_change_line("I0523 15:20:11.859251 680426 consensus_meta.cc:361] T d5398ec092d34b04ac32d540074a8a75 P 2588488096614db68658ee565bfc92fe: Updating active role from LEADER to FOLLOWER. Consensus state: current_term: 5 leader_uuid: \"\" config { peers { permanent_uuid: \"2588488096614db68658ee565bfc92fe\" member_type: VOTER last_known_private_addr { host: \"10.231.30.171\" port: 9100 } cloud_info { placement_cloud: \"onprem\" placement_region: \"dpgprod\" placement_zone: \"prodzone\" } } peers { permanent_uuid: \"a4409d981e874b73a6c98f9c1b9cae8f\" member_type: VOTER last_known_private_addr { host: \"10.231.30.173\" port: 9100 } cloud_info { placement_cloud: \"onprem\" placement_region: \"dpgprod\" placement_zone: \"prodzone\" } } }, has_pending_config = 1 "))

    configs.append(process_config_change_line("I0523 15:20:11.859251 680426 consensus_meta.cc:361] T d5398ec092d34b04ac32d540074a8a75 P 2588488096614db68658ee565bfc92fe: Updating active role from LEADER to FOLLOWER. Consensus state: current_term: 3 leader_uuid: \"\" config { peers { permanent_uuid: \"2588488096614db68658ee565bfc92fe\" member_type: VOTER last_known_private_addr { host: \"10.231.30.171\" port: 9100 } cloud_info { placement_cloud: \"onprem\" placement_region: \"dpgprod\" placement_zone: \"prodzone\" } } peers { permanent_uuid: \"a4409d981e874b73a6c98f9c1b9cae8f\" member_type: VOTER last_known_private_addr { host: \"10.231.30.173\" port: 9100 } cloud_info { placement_cloud: \"onprem\" placement_region: \"dpgprod\" placement_zone: \"prodzone\" } } }, has_pending_config = 1 "))

    configs.append(process_config_change_line("I0523 15:20:11.859190 680426 consensus_meta.cc:361] T 1149764ec6cd4306a1e9e9a4af39c32a P 2588488096614db68658ee565bfc92fe: Updating active role from LEADER to FOLLOWER. Consensus state: current_term: 4 leader_uuid: \"\" config { peers { permanent_uuid: \"2588488096614db68658ee565bfc92fe\" member_type: VOTER last_known_private_addr { host: \"10.231.30.171\" port: 9100 } cloud_info { placement_cloud: \"onprem\" placement_region: \"dpgprod\" placement_zone: \"prodzone\" } } peers { permanent_uuid: \"867fa482005e488c9869e6821daa269d\" member_type: VOTER last_known_private_addr { host: \"10.231.30.172\" port: 9100 } cloud_info { placement_cloud: \"onprem\" placement_region: \"dpgprod\" placement_zone: \"prodzone\" } } }, has_pending_config = 1 "))

    tablet_history = {}
    for config in configs:
        if config.tablet not in tablet_history:
            tablet_history[config.tablet] = TabletHistory(tablet=config.tablet, config_changes=[])
        tablet_history[config.tablet].config_changes.append(config)

    # Sort the config changes for each tablet by term and timestamp
    for tablet_configs in tablet_history.items():
        tablet_configs[1].config_changes.sort(key=lambda x: (x.term, x.timestamp))

    # Print the tablet history
    for tablet, history in tablet_history.items():
        print(f"Tablet: {tablet}")
        for change in history.config_changes:
            print(f"  Config Change: {change.timestamp} Term: {change.term} Leader UUID: {change.leader_uuid}")

def extract_config_changes(log_files):
    """
    Extract config changes from the log files.
    """
    processed = 0
    print(f"Processed ({processed}/{len(log_files)}) files")
    tablet_history = {}
    for log_file in log_files:
        sys.stdout.write("\033[F")
        print(f"Processed ({processed}/{len(log_files)}) files")
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                config_change = process_config_change_line(line)
                if config_change:
                    if config_change.tablet not in tablet_history:
                        tablet_history[config_change.tablet] = TabletHistory(tablet=config_change.tablet, config_changes=[])
                    tablet_history[config_change.tablet].config_changes.append(config_change)
        processed += 1

    # Sort the config changes for each tablet by term and timestamp
    for tablet_configs in tablet_history.items():
        tablet_configs[1].config_changes.sort(key=lambda x: (x.term, x.timestamp))

    return tablet_history

def main():
    # if len(sys.argv) != 2:
    #     print(f"Usage: {sys.argv[0]} <config_change_log_file>")
    #     sys.exit(1)

    # log_file = sys.argv[1]
    log_files = ["/Users/hsunder/Downloads/yugabyte.log.INFO.20250523-151541.677967",
                "/Users/hsunder/Downloads/yugabyte.log.INFO.20250523-150829.649961"]

    tablet_history = extract_config_changes(log_files)

    # Print the tablet history
    for tablet, history in tablet_history.items():
        print(f"Tablet: {tablet}")
        previous_change = None
        for change in history.config_changes:
            if previous_change is None or change != previous_change:
                diff = change.Diff(previous_change)
                if diff:
                    print(f"  Config Change: {diff}")
                    previous_change = change
        print("\n")

    # print(f"Time: {min_time} -> {max_time}")
    print(f"Total number of tablets: {len(tablet_history)}")
    print(f"Total number of config changes: {sum(len(history.config_changes) for history in tablet_history.values())}")

if __name__ == "__main__":
    main()