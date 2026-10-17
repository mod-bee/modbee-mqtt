// A device that registers itself with the broker - no password typed into it.
//
// The problem this solves: a site of fifty devices, each needing its own
// login, means fifty passwords to generate, type in, record, and recover when
// one is lost or a board is replaced. Instead every device is flashed with
// the SAME provisioning key, created once on the broker:
//
//   docker compose exec modbee modbee key create site-north --site north --publish 'plant/${username}/#' --subscribe 'plant/${username}/cmd'
//
// On first boot the device connects with the key, is given a login of its
// own (its name, and a secret only it holds), keeps it in flash, and connects
// as itself from then on. If an admin resets it, or the board is replaced and
// the new one registers under the same name, the old login stops working and
// this one registers again by itself - no visit to the site.
//
// The key's permissions decide what each device may do: here, publish under
// plant/<its name>/ and receive its own commands. docs/logins.md has the rest.
//
// For a modbee broker (the provisioning protocol is modbee's). ESP32: the
// login is kept in NVS with Preferences; on another board, keep it wherever
// it has room - the two functions below are all that change.

#include <WiFi.h>
#include <Preferences.h>
#include <ModBeeMqtt.h>

const char* WIFI_NAME = "plant-wifi";
const char* WIFI_PASSWORD = "";
const char* BROKER = "192.168.1.10";
const char* DEVICE_NAME = "tank-07";             // unique on the site: it becomes the user name
const char* PROVISIONING_KEY = "site-north.paste-the-key-here";

WiFiClient net;
MqttDevice mqtt(net);
Preferences flash;

float level = 0.0f;

// Where the login lives between restarts. Return true if one is stored.
bool loadLogin(void*, char* user, size_t userSize, char* password, size_t passwordSize) {
  flash.getString("user", user, userSize);
  return flash.getString("password", password, passwordSize) > 0 && user[0] != '\0';
}

// Called with the new login - and with "" and "" when it must be forgotten.
void saveLogin(void*, const char* user, const char* password) {
  flash.putString("user", user);
  flash.putString("password", password);
}

void onCommand(const char*, const MqttPayload& payload) {
  Serial.print("command: ");
  Serial.println(payload.text());
}

void setup() {
  Serial.begin(115200);
  flash.begin("modbee", false);

  WiFi.begin(WIFI_NAME, WIFI_PASSWORD);
  while (WiFi.status() != WL_CONNECTED) delay(250);

  mqtt.provision(PROVISIONING_KEY, &loadLogin, &saveLogin);
  mqtt.statusTopic("plant/tank-07/status");
  mqtt.onMessage("plant/tank-07/cmd", &onCommand);
  mqtt.report("plant/tank-07/level", &level, 0.02, 30000);

  mqtt.connect(BROKER, DEVICE_NAME);
}

void loop() {
  mqtt.poll();
  level = analogRead(A0) * (5.0f / 4095.0f);

  static MqttProvisionState shown = MqttProvisionState::Off;
  if (mqtt.provisionState() != shown) {
    shown = mqtt.provisionState();
    switch (shown) {
      case MqttProvisionState::Registering: Serial.println("registering with the broker..."); break;
      case MqttProvisionState::Pending:     Serial.println("waiting for an admin: modbee user approve tank-07"); break;
      case MqttProvisionState::Provisioned: Serial.println("has its own login"); break;
      case MqttProvisionState::Refused:     Serial.print("refused: "); Serial.println(mqtt.provisionReason()); break;
      default: break;
    }
  }
}
