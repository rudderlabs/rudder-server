import { Button, ButtonSmall } from '@components/common/button';
import { IDestinationStore } from '@stores/destination';
import Svg from '@svg/index';
import { Switch, Drawer, Popover } from 'antd';
import { observer } from 'mobx-react';
import React, { Component } from 'react';

import {
  DestinationContent,
  DestinationDetails,
  DestinationDetailsContentText,
  DestinationDetailsSourceId,
  DestinationDetailsTitleText,
  DestinationEnabled,
  DestinationHeading,
  DestinationModalView,
  DestinationNameBody,
  DestinationNameText,
  DestinationNameTitle,
  IconColumn,
  IconSpacing,
  Settings,
  TextView,
} from './styles';
import EditDestination from '../editDestination';
import DestinationIcon from '@components/icons/destinationIcon';
import { Flex } from '@components/common/misc';
import { computed } from 'mobx';
import ModalEl from '@components/common/modal';

interface IDestiantionViewProps {
  destination?: IDestinationStore;
  deleteDestination?: (destination: IDestinationStore) => any;
}
interface IDestiantionViewState {
  isSettingsDrawerVisible: boolean;
  isDeleteModalVisible: boolean;
}

const hoverTitle = 'INFO';
const hoverBody = (
  <p>
    Destination has <b>active sources</b>, <br />
    delete the connections before deleting this destination
  </p>
);

const modalTitle = 'CONFIRM DELETION';

@observer
export default class DestinationView extends Component<
  IDestiantionViewProps,
  IDestiantionViewState
> {
  constructor(props: IDestiantionViewProps) {
    super(props);
    this.state = {
      isSettingsDrawerVisible: false,
      isDeleteModalVisible: false,
    };
  }

  @computed get isDestinationConnected() {
    return this.props.destination!.sources.length > 0;
  }

  deleteDestination = (destination: IDestinationStore) => {
    console.log('destination is to be deleted');
    console.log(destination.name);
    this.setState({ isDeleteModalVisible: true });
  };

  handleOnChange = (checked: boolean, event: Event) => {
    this.props.destination!.toggleEnabled();
  };

  openSettingsDrawer = () => {
    this.setState({ isSettingsDrawerVisible: true });
  };

  handleDrawerCancel = () => {
    this.setState({ isSettingsDrawerVisible: false });
  };

  renderDestinationDetails = () => {
    const { destination } = this.props;
    const detailsObj: any = {
      'DESTINATION ID': destination!.id,
    };
    return (
      <DestinationDetails>
        {Object.keys(detailsObj).map(item => {
          return (
            <DestinationDetailsSourceId>
              <DestinationDetailsTitleText>{item}</DestinationDetailsTitleText>
              <DestinationDetailsContentText>
                {detailsObj[item]}
              </DestinationDetailsContentText>
            </DestinationDetailsSourceId>
          );
        })}
      </DestinationDetails>
    );
  };

  handleModalOk = () => {
    this.setState({ isDeleteModalVisible: false });
    this.props.deleteDestination!(this.props.destination!);
  };

  handleModalCancel = () => {
    this.setState({ isDeleteModalVisible: false });
  };

  getModalText() {
    return (
      <div
        style={{
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          height: '10vh',
        }}
      >
        <p>
          Are you sure to delete destination &nbsp;
          <b>{this.props.destination!.name}</b>?{' '}
        </p>
      </div>
    );
  }

  public render() {
    const { destination } = this.props;
    return (
      <div>
        <ModalEl
          title={modalTitle}
          showModal={this.state.isDeleteModalVisible}
          handleOk={this.handleModalOk}
          handleCancel={this.handleModalCancel}
          okText={'Confirm'}
          element={this.getModalText()}
        />
        <DestinationModalView>
          <Drawer
            visible={this.state.isSettingsDrawerVisible}
            onClose={this.handleDrawerCancel}
            width={'fit-content'}
          >
            <EditDestination
              destination={destination}
              onSaveClicked={this.handleDrawerCancel}
            ></EditDestination>
          </Drawer>
          <DestinationContent>
            <IconColumn>
              <DestinationIcon
                destination={destination!.destinationDefinition.name}
              />
            </IconColumn>

            <TextView>
              <DestinationHeading>
                <DestinationNameTitle>ANALYTICS</DestinationNameTitle>
                <Flex flexDirection="row" margin="0, 0, 15px, 0" spaceBetween>
                  <DestinationNameBody>
                    <DestinationNameText>
                      {destination!.name}
                    </DestinationNameText>
                    <DestinationEnabled>
                      <Switch
                        defaultChecked={destination!.enabled}
                        onChange={this.handleOnChange}
                        checked={destination!.enabled}
                      />
                    </DestinationEnabled>
                  </DestinationNameBody>
                </Flex>
              </DestinationHeading>

              <div>{this.renderDestinationDetails()}</div>
            </TextView>
          </DestinationContent>

          <Settings>
            <Flex flexDirection="row" alignItems="center" spaceBetween>
              {this.isDestinationConnected ? (
                <Popover content={hoverBody} title={hoverTitle}>
                  <ButtonSmall
                    disabled={this.isDestinationConnected}
                    onClick={() => this.deleteDestination(destination!)}
                  >
                    Delete Destination
                  </ButtonSmall>
                </Popover>
              ) : (
                <ButtonSmall
                  disabled={this.isDestinationConnected}
                  onClick={() => this.deleteDestination(destination!)}
                >
                  Delete Destination
                </ButtonSmall>
              )}
              <Button onClick={this.openSettingsDrawer}>
                <Svg name="settings" />
                SETTINGS
              </Button>
            </Flex>
          </Settings>
        </DestinationModalView>
      </div>
    );
  }
}
