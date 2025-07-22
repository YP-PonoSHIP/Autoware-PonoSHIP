import os
import csv
import yaml
import argparse
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
import rosbag2_py
import numpy as np
import array  # arrayモジュールをインポート

class RosbagProcessor:
    def __init__(self, bag_folder):
        self.bag_folder = bag_folder
        self.bag_file = os.path.join(bag_folder, 'metadata.yaml')  # Assuming metadata.yaml is in the bag folder
        self.storage_options = rosbag2_py.StorageOptions(uri=bag_folder, storage_id='sqlite3')
        self.converter_options = rosbag2_py.ConverterOptions('', '')

    def flatten_msg(self, msg, prefix=''):
        flat_dict = {}
        for field in msg.__slots__:
            value = getattr(msg, field)
            if hasattr(value, '__slots__'):  # Check if the field is another message
                nested_dict = self.flatten_msg(value, prefix + field + '.')
                flat_dict.update(nested_dict)
            elif isinstance(value, (list, tuple, np.ndarray, array.array)):  # Check if the field is a list, tuple, numpy array, or array.array
                # Handle empty sequences
                if len(value) == 0:
                    flat_dict[prefix + field] = []
                else:
                    for i, v in enumerate(value):
                        index = i  # Start indexing from 0
                        if hasattr(v, '__slots__'):
                            nested_dict = self.flatten_msg(v, prefix + field + f'.{index}.')
                            flat_dict.update(nested_dict)
                        else:
                            flat_dict[f"{prefix}{field}.{index}"] = v
            else:
                flat_dict[prefix + field] = value
        return flat_dict

    def extract_topic_to_csv(self, topic_name, output_csv, start_time):
        print(f"Processing topic: {topic_name}")
        reader = rosbag2_py.SequentialReader()
        reader.open(self.storage_options, self.converter_options)
        topic_types = reader.get_all_topics_and_types()

        type_dict = {topic.name: topic.type for topic in topic_types}

        msgs = []
        timestamps = []
        while reader.has_next():
            (topic, data, t) = reader.read_next()
            if topic == topic_name:
                # メッセージタイプを動的に取得
                msg_type = get_message(type_dict[topic])
                msg = deserialize_message(data, msg_type)
                msgs.append(msg)
                timestamps.append(t * 1e-9)  # Convert nanoseconds to seconds

        if not msgs:
            print(f"No messages found for topic: {topic_name}")
            return

        elapsed_times = [t - start_time for t in timestamps]

        flat_msgs = [self.flatten_msg(msg) for msg in msgs]
        # 全てのフィールド名を収集
        all_fieldnames = set()
        for flat_msg in flat_msgs:
            all_fieldnames.update(flat_msg.keys())
        fieldnames = sorted(all_fieldnames)

        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['elapsed_time', 'timestamp'] + fieldnames)
            for elapsed_time, timestamp, flat_msg in zip(elapsed_times, timestamps, flat_msgs):
                row = [elapsed_time, timestamp]
                row.extend([flat_msg.get(field, '') for field in fieldnames])
                writer.writerow(row)
        print(f"CSV output to: {output_csv}")

    def extract_topics_to_csv(self, topics, output_csv_prefix):
        # Extract the base name of the bag folder
        bag_base_name = os.path.basename(self.bag_folder)
        print("bag_base_name=" + bag_base_name)
        # Construct the output directory path
        parent_dir = os.path.dirname(os.path.dirname(self.bag_folder))
        output_dir = os.path.join(parent_dir, 'ros2csv', bag_base_name)
        print("output_dir=" + output_dir)
        os.makedirs(output_dir, exist_ok=True)

        reader = rosbag2_py.SequentialReader()
        reader.open(self.storage_options, self.converter_options)
        if not reader.has_next():
            print("Bag file is empty.")
            return
        _, _, first_timestamp = reader.read_next()
        start_time = first_timestamp * 1e-9  # Convert nanoseconds to seconds

        for topic_name in topics:
            # Replace '/' with '--' in the topic name
            sanitized_topic_name = topic_name.replace('/', '--')
            output_csv = os.path.join(output_dir, f"{sanitized_topic_name}.csv")
            # 既存のファイルがある場合はスキップ
            if os.path.exists(output_csv):
                print(f"Output file {output_csv} already exists. Skipping topic {topic_name}")
                continue
            self.extract_topic_to_csv(topic_name, output_csv, start_time)

# メインスクリプト
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert ROS2 bag file topics to CSV.')
    parser.add_argument('-b', '--bag_folder', type=str, default='your_rosbag_folder', help='Path to the ROS2 bag folder')
    args = parser.parse_args()

    with open('csv_convert_config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    bag_folder = args.bag_folder
    topics = config['topics']
    output_csv_prefix = config.get('output_csv_prefix', '')

    processor = RosbagProcessor(bag_folder)
    processor.extract_topics_to_csv(topics, output_csv_prefix)