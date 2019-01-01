import unittest
import json
import redisjq

class TestAllMethods(unittest.TestCase):
    
    def test_add_job(self):
        payload = {
            "jobs": [
                {
                    "id": "B81B43B3-EF87-455B-9D0B-1FA754AFF65A",
                    "queue_name": "test1",
                    "priority": "1",
                    "command": "ls -l"
                }
            ]
        }
        payload_json = json.dumps(payload)
        self.assertEqual(redisjq.addJob(payload_json), True)
        
if __name__ == '__main__':
    unittest.main()