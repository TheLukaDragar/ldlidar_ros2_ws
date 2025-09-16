#!/usr/bin/env python3

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import LaserScan, PointCloud2
from laser_geometry import LaserProjection
import tf2_ros
from tf2_ros import Buffer, TransformListener

class ScanToPointCloudConverter(Node):
    def __init__(self):
        super().__init__('scan_to_pointcloud_converter')
        
        # Initialize laser projection
        self.laser_projection = LaserProjection()
        
        # Initialize TF2 buffer and listener
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)
        
        # Create subscriber and publisher
        self.scan_subscriber = self.create_subscription(
            LaserScan,
            '/scan',
            self.scan_callback,
            10
        )
        
        self.pointcloud_publisher = self.create_publisher(
            PointCloud2,
            '/ambient_pointcloud',
            10
        )
        
        self.get_logger().info('Scan to PointCloud converter started')
        self.get_logger().info('Subscribing to: /scan')
        self.get_logger().info('Publishing to: /ambient_pointcloud')
    
    def scan_callback(self, scan_msg):
        try:
            # Convert LaserScan to PointCloud2
            pointcloud_msg = self.laser_projection.projectLaser(scan_msg)
            
            # Publish the converted point cloud
            self.pointcloud_publisher.publish(pointcloud_msg)
            
            self.get_logger().debug(f'Converted scan with {len(scan_msg.ranges)} points to pointcloud')
            
        except Exception as e:
            self.get_logger().error(f'Error converting scan to pointcloud: {str(e)}')

def main(args=None):
    rclpy.init(args=args)
    
    converter = ScanToPointCloudConverter()
    
    try:
        rclpy.spin(converter)
    except KeyboardInterrupt:
        pass
    finally:
        converter.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()