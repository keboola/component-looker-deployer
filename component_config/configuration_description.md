### Configurations
- mode
    - fetch_details
        - Component will run through the provided credentials and output the details of the current folder structures, dashboards and looks available from both FROM and TO Looker instances.
        - Required parameters from both environment:
            1. Base URL
            2. Client ID
            3. Client Secret
    - deploy
        - Component will deploy the selected content (dashboard, looks or folders) from FROM Looker instance into TO Looker instance

#### FROM Environment
1. Base URL
    - Your full Looker API URL
2. Client ID
3. Client Secret
4. Folder ID
    - This ID can be obtained from the Looker URL when you are exploring the folder within your Looker instance.

#### TO Environemnt
1. Base URL
    - Your full Looker API URL
2. Client ID
3. Client Secret
4. Target Folder
    - The folder in the TO Looker instance where you want to deploy
    - The component does NOT accept the folder ID for this parameter. It is required to have the full path of the folder. To get the list of folder full path, please run mode `fetch_details` to obtain such details.
5. Type
    1. dashboards
    2. looks
    3. folders
6. Value
    - You can enter the dashboards/looks/folders from your FROM looker instance which you want to deploy
    - The values are required to be the full path of the dashboards/looks/folders from the FROM Looker environment. List of full paths can be obtained by running mode `fetch_details`.
