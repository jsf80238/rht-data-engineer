# rht_data_engineer

$ grep -h "<status>" *.xml | sort | uniq
    <status>Completed</status>
    <status>In Progress</status>
    <status>Received</status>
    <status>Reopened</status>

$ grep -h "<order_id>" *.xml | sort | uniq
    <order_id>101</order_id>
    <order_id>102</order_id>
    <order_id>103</order_id>
    <order_id>104</order_id>
